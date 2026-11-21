export function canDisplayCredentialVersion(currentVersion: number, candidateVersion: number): boolean {
  return Number.isSafeInteger(candidateVersion) && candidateVersion > 0 && candidateVersion >= currentVersion;
}

export function highestCredentialVersion(currentVersion: number, candidateVersion: number): number {
  return canDisplayCredentialVersion(currentVersion, candidateVersion)
    ? Math.max(currentVersion, candidateVersion)
    : currentVersion;
}

export function ticketCredentialVersionStorageKey(ticketId: string): string {
  return `rushgo:ticket-credential-version:${ticketId}`;
}

export function ticketCredentialPendingStorageKey(ticketId: string): string {
  return `rushgo:ticket-credential-pending:${ticketId}`;
}
