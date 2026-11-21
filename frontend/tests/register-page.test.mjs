import assert from "node:assert/strict";
import { test } from "node:test";
import { componentDriver, find } from "./recovery-driver.mjs";
import { loadTypeScript } from "./render-typescript.mjs";

const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function proof(verificationId) {
  return {
    verification_id: verificationId,
    expires_at: new Date(Date.now() + 10 * 60_000).toISOString(),
    next_resend_at: new Date(Date.now() + 60_000).toISOString(),
  };
}

// This hook driver is a synthetic component harness. Actual React/browser
// keyboard and focus behavior is covered by the isolated browser checks.
function registerPageFixture({ requestRegistrationCode, register, locale = "en-US", query = new URLSearchParams() } = {}) {
  const requests = [];
  const registrations = [];
  const pushes = [];
  const apiClient = {
    requestRegistrationCode(email) {
      requests.push(email);
      return requestRegistrationCode
        ? requestRegistrationCode(email)
        : Promise.resolve(proof("proof-" + email));
    },
  };
  const registerUser = (input) => {
    registrations.push(input);
    return register ? register(input, registrations.length) : Promise.resolve({});
  };
  const component = componentDriver("app/(customer)/register/page.tsx", {
    "next/navigation": { useRouter: () => ({ push: (href) => pushes.push(href) }), useSearchParams: () => query },
    "@/lib/auth-context": { useAuth: () => ({ register: registerUser }) },
    "@/lib/api/client": { apiClient },
    "@/lib/i18n/index": { useI18n: () => getI18n(locale) },
  });

  const driver = component;
  const field = (id) => find(driver.render(), (node) => node.props?.id === id);
  const change = (id, value) => {
    const input = field(id);
    assert(input, "missing input " + id);
    input.props.onChange({ target: { value } });
  };
  const sendButton = () =>
    find(driver.render(), (node) => node.type === "button" && node.props.type === "button");
  const submitForm = () => find(driver.render(), (node) => node.type === "form");

  return { driver, field, change, sendButton, submitForm, requests, registrations, pushes };
}

test("changing email clears proof and code while preserving registration fields", async () => {
  const fixture = registerPageFixture();
  fixture.change("register-name", "Synthetic User");
  fixture.change("register-email", "first@example.test");
  fixture.change("register-password", "a synthetic long password");
  fixture.change("register-confirm", "a synthetic long password");

  await fixture.sendButton().props.onClick();
  assert(find(fixture.driver.render(), (node) => node.props?.role === "status"));
  fixture.change("register-code", "000031");

  fixture.change("register-email", "second@example.test");

  assert.equal(fixture.field("register-code").props.value, "");
  assert.equal(find(fixture.driver.render(), (node) => node.props?.role === "status"), null);
  assert.equal(fixture.field("register-name").props.value, "Synthetic User");
  assert.equal(fixture.field("register-password").props.value, "a synthetic long password");
  assert.equal(fixture.field("register-confirm").props.value, "a synthetic long password");
  assert.equal(fixture.sendButton().props.disabled, false);
});

test("stale verification response cannot replace or unblock a newer email request", async () => {
  const oldResponse = deferred();
  const currentResponse = deferred();
  const fixture = registerPageFixture({
    requestRegistrationCode: (email) =>
      email === "old@example.test" ? oldResponse.promise : currentResponse.promise,
  });

  fixture.change("register-email", "old@example.test");
  const oldRequest = fixture.sendButton().props.onClick();

  fixture.change("register-email", "current@example.test");
  const currentButton = fixture.sendButton();
  const canSendCurrentEmail = currentButton.props.disabled === false;
  const currentRequest = canSendCurrentEmail
    ? fixture.sendButton().props.onClick()
    : null;

  oldResponse.reject(new Error("Old address mail failed"));
  await oldRequest;

  const alert = find(fixture.driver.render(), (node) => node.props?.role === "alert");
  const currentButtonAfterStaleFailure = fixture.sendButton();
  const currentRequestStillPending =
    currentRequest !== null &&
    currentButtonAfterStaleFailure.props.disabled === true;
  const currentRequestShowsSending =
    currentRequest !== null &&
    currentButtonAfterStaleFailure.props.children === getI18n("en-US").t("customer.auth.sending");

  if (currentRequest) {
    currentResponse.resolve(proof("current-proof"));
    await currentRequest;
  }

  const proofVisible = find(fixture.driver.render(), (node) => node.props?.role === "status") !== null;
  assert.deepEqual(
    {
      canSendCurrentEmail,
      staleError: alert?.props.children ?? null,
      currentRequestStillPending,
      currentRequestShowsSending,
      requests: fixture.requests,
      proofVisible,
    },
    {
      canSendCurrentEmail: true,
      staleError: null,
      currentRequestStillPending: true,
      currentRequestShowsSending: true,
      requests: ["old@example.test", "current@example.test"],
      proofVisible: true,
    },
  );
});

test("verification request shows the localized Sending label while pending", async () => {
  const currentResponse = deferred();
  const fixture = registerPageFixture({
    locale: "zh-CN",
    requestRegistrationCode: () => currentResponse.promise,
  });

  fixture.change("register-email", "current@example.test");
  const currentRequest = fixture.sendButton().props.onClick();
  const pendingButton = fixture.sendButton();

  assert.equal(pendingButton.props.disabled, true);
  assert.equal(pendingButton.props.children, getI18n("zh-CN").t("customer.auth.sending"));

  currentResponse.resolve(proof("current-proof"));
  await currentRequest;
});

test("failed registration keeps its verification proof available for one retry", async () => {
  const firstAttempt = deferred();
  const fixture = registerPageFixture({
    register: (_input, attempt) =>
      attempt === 1 ? firstAttempt.promise : Promise.resolve({ account_created: true }),
  });

  fixture.change("register-name", "Synthetic User");
  fixture.change("register-email", "retry@example.test");
  fixture.change("register-password", "a synthetic long password");
  fixture.change("register-confirm", "a synthetic long password");
  await fixture.sendButton().props.onClick();
  fixture.change("register-code", "000041");

  const failedSubmit = fixture.submitForm().props.onSubmit({ preventDefault() {} });
  assert.equal(
    find(fixture.driver.render(), (node) => node.type === "button" && node.props.type === "submit").props.disabled,
    true,
  );

  firstAttempt.reject(new Error("Synthetic registration outage"));
  await failedSubmit;

  assert.equal(
    find(fixture.driver.render(), (node) => node.props?.role === "alert")?.props.children,
    "Something went wrong. Please try again.",
  );
  assert.equal(fixture.field("register-code").props.value, "000041");
  assert(find(fixture.driver.render(), (node) => node.props?.role === "status"));

  await fixture.submitForm().props.onSubmit({ preventDefault() {} });

  assert.equal(fixture.registrations.length, 2);
  assert.equal(fixture.registrations[0].verification_id, "proof-retry@example.test");
  assert.equal(fixture.registrations[1].verification_code, "000041");
  assert.deepEqual(fixture.pushes, ["/login?success=registered"]);
});

test("registration preserves only a safe seat return through account creation and the login link", async () => {
  const query = new URLSearchParams({ redirect: "/events/event-1/seats" });
  const fixture = registerPageFixture({ query, register: async () => ({ account_created: true }) });
  const signIn = find(fixture.driver.render(), (node) => node.props?.href?.startsWith("/login?"));
  assert.equal(signIn.props.href, "/login?redirect=%2Fevents%2Fevent-1%2Fseats");
  fixture.change("register-name", "Synthetic User");
  fixture.change("register-email", "resume@example.test");
  fixture.change("register-password", "a sufficiently long password");
  fixture.change("register-confirm", "a sufficiently long password");
  await fixture.sendButton().props.onClick();
  fixture.change("register-code", "000042");
  await fixture.submitForm().props.onSubmit({ preventDefault() {} });
  const target = new URL(fixture.pushes[0], "https://rushgo.example");
  assert.equal(target.pathname, "/login");
  assert.equal(target.searchParams.get("success"), "registered");
  assert.equal(target.searchParams.get("redirect"), "/events/event-1/seats");
});

test("registration normalizes an unsafe return to the local root", async () => {
  const query = new URLSearchParams({ redirect: "//evil.example/path" });
  const fixture = registerPageFixture({ query, register: async () => ({ account_created: true }) });
  const signIn = find(fixture.driver.render(), (node) => node.props?.href?.startsWith("/login?"));
  assert.equal(signIn.props.href, "/login?redirect=%2F");
  fixture.change("register-name", "Synthetic User");
  fixture.change("register-email", "safe@example.test");
  fixture.change("register-password", "a sufficiently long password");
  fixture.change("register-confirm", "a sufficiently long password");
  await fixture.sendButton().props.onClick();
  fixture.change("register-code", "000043");
  await fixture.submitForm().props.onSubmit({ preventDefault() {} });
  assert.equal(new URL(fixture.pushes[0], "https://rushgo.example").searchParams.get("redirect"), "/");
});
