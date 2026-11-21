export function isCredentialRedirect(url: URL): boolean {
  return ["/login", "/forgot-password", "/reset-password"].includes(url.pathname) || !!url.hash || /(?:token|password|code)=/i.test(url.search);
}
export function isPublicRecoveryPath(pathname: string): boolean {
  return ["/login", "/forgot-password", "/reset-password"].includes(pathname);
}
