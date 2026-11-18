import { createHttpApp } from './interfaces/http/app';

const app = await createHttpApp();
await app.listen(Number(process.env.PORT ?? 3000));
