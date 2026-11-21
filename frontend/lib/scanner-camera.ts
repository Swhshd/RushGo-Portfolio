export type CameraError = "frame" | "unsupported" | "permission" | "insecure" | "device" | "busy";

export function cameraFailure(error: unknown): CameraError {
  const name = error instanceof Error ? error.name : "";
  if (name === "NotAllowedError" || name === "SecurityError") return "permission";
  if (name === "NotFoundError" || name === "OverconstrainedError") return "device";
  if (name === "NotReadableError" || name === "AbortError") return "busy";
  return "unsupported";
}
