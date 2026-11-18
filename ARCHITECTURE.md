# Arquitetura e decisões verificadas

Fonte: README oficial do desafio, commit `c7143e6d041585d6913f56eb7415105e074f83dc`. As limitações conhecidas estão ao final.

## Invariantes e fronteiras

- Dinheiro entra/sai como string decimal de duas casas, com moeda ISO-4217; domínio usa centavos inteiros em `bigint`, nunca `number`.
- Wallet é agregado por `(playerId,currency)`. Saldo materializado deve coincidir com soma ordenada do ledger.
- Nenhuma movimentação deixa saldo negativo. Cada operação com efeito gera exatamente um lançamento; `LOSS` e rejeições não geram.
- Ledger é append-only. Estados terminais de transação não mudam.
- Operação financeira, ledger, inbox (quando aplicável) e outbox são escritos na mesma transação SQL.
- Publicação de eventos ocorre após commit, com semântica at-least-once.

## Persistência e concorrência

MikroORM 6.x foi escolhido por ser a preferência oficial e suportar transações e migrations. As classes de domínio independem do ORM; o serviço as reidrata a partir das linhas retornadas por SQL parametrizado no `EntityManager` transacional. `Money` armazena centavos em `bigint` e o banco usa `NUMERIC(20,2)`; nenhuma conversão monetária passa por `number`. A faixa máxima é 18 dígitos inteiros e duas casas, validada no domínio. `CHECK`, `UNIQUE`, FKs e triggers cobrem saldo não negativo, unicidade, versão, integridade aritmética, vínculo do ledger e imutabilidade de ledger/transações terminais. As migrations têm `up` e `down` explícitos.

Comandos verificam primeiro a identidade idempotente, permitindo replay/conflito mesmo se a wallet informada não existir. Operações novas bloqueiam a linha da wallet com `FOR UPDATE` e repetem a consulta idempotente sob o lock, preservando serialização e evitando corrida entre envios simultâneos. Índices únicos também protegem a identidade e a transação externa. SQLSTATE `40P01`, `40001`, `08000`, `08003`, `08006` e `57P01` recebem no máximo três tentativas com atraso exponencial curto e jitter; erros de negócio não são repetidos. Não há lock global. A versão começa em 1 e só sobe quando saldo muda. O banco verifica essa regra também para `UPDATE` direto.

## Contratos e idempotência

HTTP e SQS chamam o mesmo caso de uso. `Idempotency-Key` é obrigatório no HTTP. A identidade persistente combina provider e key; também há unicidade em provider e externalTransactionId. O hash SHA-256 usa JSON canônico com ordenação recursiva das chaves do objeto formado por `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money` e `referenceExternalTransactionId` (ou `null`). Header e metadados de transporte não entram. Replay devolve o snapshot original com `idempotentReplay=true`, inclusive o saldo histórico. Mesmo externalId com outra key é conflito. Se a resposta inicial foi `PENDING_REFERENCE`, o replay conserva essa resposta; as consultas de transação mostram o estado atual.

Os status HTTP planejados são 400 para payload inválido, 409 para conflito de identidade, 422 para regra financeira rejeitada, 202 para pendência de referência e 503 para infraestrutura transitória. O contrato JSON inclui código estável, mensagem e correlationId.

## Reversões e referências

`BET` debita; `WIN` credita; `LOSS` registra sem movimento; `REFUND` credita uma BET; `ROLLBACK` inverte BET, WIN ou REFUND. Referência é resolvida por `(providerId,externalTransactionId)` e exige mesmo player, wallet, moeda e rodada. Valor da reversão é integral. Cada tipo pode reverter uma referência no máximo uma vez. A reversão de outro tipo só é permitida quando o efeito da reversão anterior foi desfeito por `ROLLBACK` do `REFUND`, evitando crédito duplicado. Reversão que tornaria saldo negativo recebe código distinto de BET sem saldo.

Exemplo: saldo inicial 100, BET de 25 deixa 75 e REFUND devolve 100. Um ROLLBACK da mesma BET é rejeitado enquanto o REFUND estiver ativo; após ROLLBACK do REFUND debitar 25, o ROLLBACK da BET pode creditar 25. Repetir o mesmo tipo sobre a mesma referência continua proibido.

Referências que ainda não existem ou continuam `PENDING_REFERENCE` mantêm a operação dependente em espera. O worker reavalia a cadeia conforme cada referência avança, evitando rejeitar cedo um `ROLLBACK → REFUND → BET` válido.

Referência ausente ou ainda pendente persiste `PENDING_REFERENCE`; o worker tenta novamente com atrasos 2/4/8/16 segundos e rejeita na quinta verificação sem referência processada, com `REFERENCE_NOT_FOUND` e evento. Isso também limita ciclos de referências; cadeias legítimas que não terminem dentro desse prazo são rejeitadas. `OPENING` é interno; wallet inicial zero não tem transação nem ledger. `WIN` pode não ter referência; se tiver, exige BET processada do mesmo contexto.

## Mensageria e falhas

Inbox deduplica `(consumerName,messageId)` na mesma transação financeira. O consumer faz ack só após commit; erro de negócio que retorna `REJECTED` recebe ack; exceções mantêm a mensagem para retry com visibilidade exponencial. A fila FIFO principal tem DLQ FIFO com `maxReceiveCount=5`. SIGTERM interrompe polling depois de concluir a mensagem em andamento. Falha de transporte no polling recebe retry limitado por intervalo, sem encerrar o loop.

Outbox é escrita no commit financeiro. Publishers concorrentes reservam **uma linha** com `FOR UPDATE SKIP LOCKED` em uma transação separada; enviam para SQS enquanto mantêm o lock da linha e a marcam publicada antes de concluir essa transação. Crash entre envio e commit pode duplicar publicação, então consumidores devem usar `eventId` para dedup persistente. Eventos têm classe abstrata, subclasses concretas, envelope `version=1` e `MoneyProps` serializado. A chamada de rede sob lock prolonga a reserva da linha quando SQS está lento; o lock é por evento, não por wallet nem global.

## Autenticação, observabilidade e limitações

Autenticação não é pontuada no desafio. Nesta entrega, a escolha inicial é ponto de extensão `ProviderIdentityPort`/guard sem IdP; a integração futura seria OIDC com Keycloak/Zitadel. Health é público e SQS é canal interno, mas providerId da mensagem passa pelas regras de domínio.

Logs JSON incluem IDs operacionais sem payload financeiro completo. Cada requisição HTTP recebe ou gera um `correlationId`, propagado aos logs e devolvido no header e nas respostas de erro. Métricas do processo registram status, duplicatas, retries, quantidade atual na DLQ, conflitos de lock, latência e lag da outbox. Liveness verifica processo; readiness verifica PostgreSQL e SQS. Divergência na reconciliação gera resposta `consistent=false`, log JSON e contador; não corrige o saldo.

## Códigos de falha

| Código | Significado | Ação recomendada ao provedor |
|---|---|---|
| `INSUFFICIENT_BALANCE` | BET excede saldo | Corrigir valor ou aguardar crédito |
| `REVERSAL_INSUFFICIENT_BALANCE` | Reversão debitora deixaria saldo negativo | Intervenção operacional; não repetir cegamente |
| `INVALID_REFERENCE` | Tipo, estado, identidade, moeda, rodada ou valor da referência incompatível | Corrigir payload/referência |
| `ALREADY_REVERSED` | Referência já tem reversão processada | Não reenviar outra reversão |
| `REFERENCE_NOT_FOUND` | Referência não apareceu após cinco verificações | Investigar ordem/ID e contatar operação |
| `AMOUNT_OUT_OF_RANGE` | A liquidação pendente excederia a faixa monetária suportada | Investigar o saldo e o valor antes de enviar nova operação |

Erros de validação na entrada (`INVALID_AMOUNT`, `INVALID_CURRENCY`, `REFERENCE_REQUIRED`), conflito de idempotência (`IDEMPOTENCY_CONFLICT`) e indisponibilidade de infraestrutura não geram transação rejeitada. Se dados persistidos de uma pendência violarem invariantes monetárias, o worker rejeita a operação com o código de domínio correspondente. HTTP diferencia 400, 409, 422, 202, 500 e 503.

## Limitações conhecidas

- MikroORM fornece transações e migrations, mas a persistência usa SQL parametrizado em vez de mapeamento de entidades. Isso preserva o domínio independente do ORM e facilita constraints explícitas, ao custo de mais código de adaptação e menor uso de Unit of Work.
- Métricas são locais ao processo; agregação entre instâncias depende de um coletor externo. OpenTelemetry/dashboard não foram implementados por serem opcionais.
- O consumer usa o redrive do SQS para mover mensagens inválidas à DLQ após o limite. A falha específica é registrada em log, mas não há reprocessamento manual da DLQ pela aplicação.
- A API usa validação manual de DTOs. Os endpoints de transações retornam DTOs explícitos, mas ainda falta ampliar a validação negativa de todos os campos e limites de tamanho.
- Reverter a migration 005 exige que não existam reversões de tipos distintos para a mesma referência, pois a versão anterior possui uma restrição mais estrita. A reversão falha sem excluir dados caso essa condição não seja satisfeita.

A consulta de DLQ em `/metrics` retorna `dlq: null` e `dlqCollectionError: true` quando a coleta falha. Cada worker expõe um endpoint próprio na rede interna conforme documentado no README. As métricas são locais e reiniciam com o processo; agregação e identificação das réplicas são responsabilidade do coletor.
