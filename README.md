# Jungle Gaming — Distributed Wagering Processor

Implementação do [desafio oficial](https://github.com/junglegaming/backend-challenge) no commit `c7143e6d041585d6913f56eb7415105e074f83dc`. Consulte [ARCHITECTURE.md](ARCHITECTURE.md) para decisões, invariantes e limitações.

O MikroORM gerencia conexões, transações e migrations; o ledger usa SQL parametrizado dentro das transações para explicitar locks pessimistas e escritas. Os valores monetários usam `bigint` em centavos no domínio e `NUMERIC` no PostgreSQL, sem conversão monetária para `number`. O ledger não é mapeado como entidade: sua imutabilidade é protegida por trigger no banco, sem mutações acidentais pelo Identity Map.

`BET` e `LOSS` não aceitam `referenceExternalTransactionId`; sua presença é rejeitada com `REFERENCE_NOT_ALLOWED`. `WIN` pode referenciar uma `BET`, enquanto `REFUND` e `ROLLBACK` exigem uma referência.

Replays retornam a resposta inicial, inclusive quando estava pendente; consulte a transação para acompanhar seu estado atual. Cada tipo de reversão pode atuar uma vez sobre uma referência e não pode duplicar uma reversão financeira ainda ativa. O header `x-correlation-id` é aceito ou gerado para correlacionar requisições HTTP e logs.

## Requisitos de ambiente

- Bun 1.x como runtime, gerenciador e test runner. No PowerShell desta máquina, invoque `bun.cmd`; em Linux/macOS, use `bun`.
- Docker Desktop/Engine com Linux containers e Docker Compose.
- Portas locais 5439 (PostgreSQL), 4566 (LocalStack) e 3000 (API) livres.

## Iniciar tudo com Docker Compose

```powershell
docker compose --profile app up -d --build --wait
Invoke-RestMethod http://localhost:3000/health/ready
docker compose --profile app ps
```

O perfil `app` sobe PostgreSQL, LocalStack, migration, API, consumer SQS, publisher da outbox e worker de referências pendentes. A API fica em `http://localhost:3000`. `GET /health/live` verifica o processo; `GET /health/ready` verifica PostgreSQL e SQS. `GET /metrics` devolve o snapshot de métricas do processo. Para acompanhar logs JSON:

```powershell
docker compose --profile app logs -f api consumer outbox pending
```

Para parar os serviços sem apagar dados:

```powershell
docker compose --profile app down
```

## Executar localmente com Bun

```powershell
bun.cmd install
docker compose up -d --wait
bun.cmd scripts/migrate.ts
bun.cmd src/main.ts
```

Em terminais separados, execute os workers quando precisar do fluxo assíncrono:

```powershell
bun.cmd src/workers/run-consumer.ts
bun.cmd src/workers/run-outbox.ts
bun.cmd src/workers/run-pending.ts
```

Valores padrão: `DATABASE_URL=postgresql://jungle:jungle@localhost:5439/jungle`, `SQS_ENDPOINT=http://localhost:4566`, `AWS_REGION=us-east-1`, credenciais locais `test/test`. Em Compose, os endereços internos são configurados automaticamente. O consumer cria `wager-transactions.fifo` e `wager-transactions-dlq.fifo`; o publisher cria `wager-events.fifo`.

## Migrations

```powershell
bun.cmd scripts/migrate.ts
bun.cmd scripts/migrate.ts down
```

`down` reverte a migration mais recente. `bun.cmd scripts/migrate.ts down-all` reverte **todas** e apaga o schema da aplicação; use somente num banco descartável. A sequência completa `up → down-all → up` foi verificada num banco separado e removido após a prova.

## Testes e verificação

Os testes de integração usam **PostgreSQL e LocalStack reais**. Execute-os com apenas as dependências do Compose; pare API e workers do perfil `app` antes, pois eles poderiam consumir mensagens criadas pelos testes:

```powershell
docker compose --profile app stop api consumer outbox pending
docker compose up -d --wait
bun.cmd run typecheck
bun.cmd test
```

Para executar grupos específicos:

```powershell
bun.cmd test tests/unit
bun.cmd test tests/integration/schema.test.ts
bun.cmd test tests/integration/concurrency.test.ts
bun.cmd test tests/integration/multiprocess.test.ts
bun.cmd test tests/integration/sqs.test.ts
bun.cmd test tests/integration/outbox.test.ts
```

A suíte cobre precisão monetária, invariantes, migrations/constraints, HTTP, 50 replays simultâneos, duas apostas disputando saldo, três processos reais, wallets distintas, redelivery após crash antes do ack, DLQ, dois publishers, referência fora de ordem, reinício e reconciliação. `tests/integration/reconciliation.test.ts` procura qualquer wallet divergente no banco de teste. Um banco antigo contaminado por testes interrompidos pode fazê-lo falhar; o resultado exige investigação, nunca correção silenciosa.

## Exemplos HTTP

```powershell
$playerId = [guid]::NewGuid().ToString()
$wallet = Invoke-RestMethod -Method Post -Uri 'http://localhost:3000/wallets' -ContentType 'application/json' -Body (@{ playerId = $playerId; initialBalance = @{ amount = '100.00'; currency = 'BRL' } } | ConvertTo-Json -Depth 4)
$externalId = [guid]::NewGuid().ToString()
$bet = Invoke-RestMethod -Method Post -Uri 'http://localhost:3000/wagering/transactions' -Headers @{ 'Idempotency-Key' = $externalId } -ContentType 'application/json' -Body (@{ providerId = 'provider-a'; externalTransactionId = $externalId; playerId = $playerId; walletId = $wallet.id; roundId = 'round-1'; gameId = 'game-1'; kind = 'BET'; money = @{ amount = '10.00'; currency = 'BRL' } } | ConvertTo-Json -Depth 4)
$check = Invoke-RestMethod -Method Post -Uri "http://localhost:3000/wallets/$($wallet.id)/reconciliation"
$bet
$check
```

Consultas: `GET /wallets/:walletId`, `GET /wallets/:walletId/ledger?limit=50&cursor=...`, `GET /wagering/transactions/:transactionId` e `GET /providers/:providerId/wagering/transactions/:externalTransactionId`. O cursor de ledger é opaco para clientes; use `nextCursor` recebido na página anterior.

## SQS

O consumer aceita o envelope `WagerTransactionRequested` do enunciado oficial, com `messageId`, `occurredAt` e `data` contendo a operação e `idempotencyKey`. Envie à fila FIFO com `MessageGroupId=walletId` e `MessageDeduplicationId=messageId`. A garantia final de deduplicação é a inbox e a transação SQL, independente do recurso FIFO. Eventos versionados são publicados em `wager-events.fifo` pelo publisher da outbox.

Uma rejeição de negócio persistida é confirmada na fila depois do commit. Erros permanentes de entrada ou identidade, erros transitórios e erros desconhecidos não recebem ack: a visibilidade cresce exponencialmente e o SQS encaminha a mensagem à DLQ após cinco recebimentos, inclusive se uma indisponibilidade transitória durar mais que esse limite. Os logs identificam a classe; a recuperação da DLQ exige intervenção operacional. Eventos podem ser publicados novamente após interrupção e consumidores precisam deduplicar por `eventId`.

Cada worker expõe `GET /metrics` na rede do Compose: `consumer:9464`, `outbox:9465` e `pending:9466`; a API expõe `api:3000/metrics`. Um coletor conectado a essa rede pode consultar cada instância e agregar os resultados com sua identidade de instância. Em execução local, use `http://localhost:9464/metrics`, `:9465/metrics` e `:9466/metrics`; `METRICS_PORT` permite alterar a porta de cada processo. Os contadores são locais e reiniciam com o processo; o coletor deve acompanhar reinícios e substituições de instâncias. A coleta da DLQ ocorre apenas na API, portanto nos workers `dlq` é `null`. Não exponha essas portas publicamente sem controles de acesso.

## Status e limites

A autenticação OIDC foi adiada conforme liberdade do desafio; o código contém um guard de extensão. A API usa validação manual de DTOs.

### Contratos e interpretação das métricas

A API e a fila compartilham a validação de negócio em `WagerService`: identificadores são strings não vazias sem espaços nas pontas (provider até 128 caracteres; demais identificadores e chave até 256), valores monetários são strings decimais com duas casas e moeda ISO suportada. Valores de aposta devem ser positivos; o saldo inicial pode ser zero. A fila exige também `messageId` não vazio de até 256 caracteres, tipo `WagerTransactionRequested` e `occurredAt` ISO 8601 com fuso. Campos adicionais são ignorados; somente o subconjunto de negócio documentado participa do hash. IDs de wallet e transação são armazenados como texto, portanto IDs desconhecidos retornam ausência e não precisam ter formato UUID.

Os contadores são locais a cada processo: `transactionStatuses` conta respostas de submissão, inclusive replays, e transições terminais confirmadas pelo worker de pendências; `duplicates` conta replays; `processingLatencyMs` mede essas submissões. `retries` conta tentativas reagendadas ou falhas dos loops, e `lockConflicts` conta deadlocks e falhas de serialização, inclusive na última tentativa: espera normal por lock não é contabilizada. A API calcula `outboxLagMs` pela mensagem pendente mais antiga; no publisher é a idade da última mensagem selecionada, não uma medição contínua da fila inteira. A DLQ é consultada pela API; falha de coleta é indicada por `dlqCollectionError`, e workers retornam `dlq: null`. Os estados de domínio `PENDING` e `FAILED` não são emitidos pelo fluxo de aplicação atual e não aparecem na métrica. Os contadores não representam um censo histórico de transações e reiniciam com cada processo.
