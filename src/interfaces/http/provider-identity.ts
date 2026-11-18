import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';

/** Extension point for future OIDC token validation and provider binding. */
export interface ProviderIdentityPort {
  authorize(providerId: string, token: string | undefined): Promise<boolean>;
}

/** Challenge decision: authentication integration is deferred as the official README permits. */
@Injectable()
export class NoopProviderIdentityGuard implements CanActivate {
  canActivate(_context: ExecutionContext): boolean { return true; }
}
