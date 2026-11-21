"use client";

import React, { createContext, useContext, useState, useEffect, useCallback } from "react";
import { apiClient } from "./api/client";
import { useRouter } from "next/navigation";
import { loginHref } from "./auth-navigation";
import { isPublicRecoveryPath } from "./recovery-navigation";
import type { ApiRegisterInput, ApiRegistrationResponse, ApiUser } from "./api/types";
import { setLocale } from "./i18n/locale-store";

interface User {
  id: string;
  email: string;
  name: string;
  role: string;
  locale?: "en" | "zh" | string;
}

interface AuthState {
  user: User | null;
  isLoading: boolean;
  login: (email: string, password: string) => Promise<User>;
  register: (input: ApiRegisterInput) => Promise<ApiRegistrationResponse>;
  resetPassword: (token: string, newPassword: string) => Promise<void>;
  logout: () => Promise<void>;
  clearSession: () => void;
  syncUser: (profile: ApiUser) => void;
}

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [isLoading, setIsLoading] = useState(() => apiClient.isLoggedIn());

  const loadProfileForSession = useCallback(async (expectedVersion: number, isCurrent: () => boolean) => {
    try {
      const profile = await apiClient.getProfile();
      if (isCurrent() && apiClient.getSessionVersion() === expectedVersion && apiClient.isLoggedIn()) {
        setUser({ id: profile.id, email: profile.email, name: profile.name, role: profile.role, locale: profile.locale });
        if (profile.locale === "en" || profile.locale === "zh") setLocale(profile.locale === "zh" ? "zh-CN" : "en-US");
      }
    } catch {
      if (isCurrent() && apiClient.getSessionVersion() === expectedVersion) setUser(null);
    } finally {
      if (isCurrent() && apiClient.getSessionVersion() === expectedVersion) setIsLoading(false);
    }
  }, []);

  // Check the current session on mount and reconcile changes made in another tab.
  useEffect(() => {
    let current = true;
    const unsubscribe = apiClient.subscribeSessionExpired(() => {
      setUser(null);
      setIsLoading(false);
      if (!isPublicRecoveryPath(window.location.pathname)) {
        router.push(loginHref(window.location.pathname + window.location.search, "SESSION_EXPIRED"));
      }
    });
    const handleStorage = (event: StorageEvent) => {
      if (event.key !== "access_token" && event.key !== null) return;
      const version = apiClient.notifyExternalSessionChange();
      setUser(null);
      const loggedIn = apiClient.isLoggedIn();
      setIsLoading(loggedIn);
      if (loggedIn) void loadProfileForSession(version, () => current);
    };
    window.addEventListener("storage", handleStorage);
    const initialVersion = apiClient.getSessionVersion();
    if (apiClient.isLoggedIn()) {
      void Promise.resolve().then(() => loadProfileForSession(initialVersion, () => current));
    }
    return () => { current = false; window.removeEventListener("storage", handleStorage); unsubscribe(); };
  }, [router, loadProfileForSession]);

  const login = useCallback(async (email: string, password: string) => {
    const res = await apiClient.login(email, password);
    const user = { id: res.user.id, email: res.user.email, name: res.user.name, role: res.user.role, locale: res.user.locale };
    if (res.user.locale === "en" || res.user.locale === "zh") setLocale(res.user.locale === "zh" ? "zh-CN" : "en-US");
    setUser(user);
    setIsLoading(false);
    return user;
  }, []);

  const register = useCallback(async (input: ApiRegisterInput) => {
    return apiClient.register(input);
  }, []);
  const resetPassword = useCallback(async (token: string, password: string) => { await apiClient.resetPassword(token, password); apiClient.clearLocalSession(); setUser(null); }, []);

  const logout = useCallback(async () => {
    const version = apiClient.getSessionVersion();
    try { await apiClient.logout(); } finally { if (apiClient.getSessionVersion() === version + 1 && !apiClient.isLoggedIn()) setUser(null); }
  }, []);

  const clearSession = useCallback(() => {
    apiClient.clearLocalSession();
    setUser(null);
    setIsLoading(false);
  }, []);
  const syncUser = useCallback((profile: ApiUser) => {
    setUser({ id: profile.id, email: profile.email, name: profile.name, role: profile.role, locale: profile.locale });
  }, []);

  return (
    <AuthContext.Provider value={{ user, isLoading, login, register, logout, resetPassword, clearSession, syncUser }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within AuthProvider");
  return ctx;
}
