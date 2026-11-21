export function validateNewPassword(password: string): string | null {
  const points = Array.from(password);
  if (points.some(point => { const code = point.codePointAt(0)!; return code >= 0xd800 && code <= 0xdfff; })) return "Please enter valid text.";
  if (points.length < 15) return "Use at least 15 characters.";
  if (new TextEncoder().encode(password).length > 72) return "This password is too long. Use fewer characters.";
  return null;
}
