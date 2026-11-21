import { ApiError } from "./api/errors";
import type { ApiAdminFeatureFlagsResponse, ApiFeatureFlag } from "./api/types";

export const allowedAdminFeatureFlagKeys = ["discovery_advanced_filters"] as const;

export function isAllowedAdminFeatureFlag(key: string): boolean {
  return (allowedAdminFeatureFlagKeys as readonly string[]).includes(key);
}

export async function updateFeatureFlagWithConflictReload(
  current: ApiFeatureFlag,
  update: (key: string, enabled: boolean, expectedVersion: number) => Promise<unknown>,
  reload: () => Promise<ApiAdminFeatureFlagsResponse>,
): Promise<{ state: "updated" } | { state: "conflict"; latest: ApiAdminFeatureFlagsResponse | null }> {
  try {
    await update(current.key, !current.enabled, current.version);
    return { state: "updated" };
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 409) throw error;
    try {
      return { state: "conflict", latest: await reload() };
    } catch {
      return { state: "conflict", latest: null };
    }
  }
}
