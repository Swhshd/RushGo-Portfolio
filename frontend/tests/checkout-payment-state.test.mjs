import assert from "node:assert/strict";
import { test } from "node:test";
import React from "react";
import { browserOverrides, loadTypeScript } from "./render-typescript.mjs";
const { getI18n } = loadTypeScript("lib/i18n/core.ts");

function paymentForm(confirmCardPayment, onSuccess = () => {}, onError = () => {}, availability = {}) {
  const slots = [];
  const refs = [];
  let cursor = 0;
  const hooks = {
    ...React,
    useContext(context) { return context._currentValue; },
    useState(initial) {
      const index = cursor++;
      if (!(index in slots)) slots[index] = initial;
      return [slots[index], (value) => { slots[index] = typeof value === "function" ? value(slots[index]) : value; }];
    },
    useRef(initial) {
      const index = cursor++;
      if (!(index in refs)) refs[index] = { current: initial };
      return refs[index];
    },
  };
  const { StripePaymentForm } = loadTypeScript("components/stripe-payment-form.tsx", {
    ...browserOverrides,
    react: hooks,
    "@/lib/i18n/index": { useI18n: () => getI18n(availability.locale ?? "en-US") },
    "@stripe/react-stripe-js": {
      Elements: ({ children }) => children,
      CardElement: () => React.createElement("div", null, "Card input"),
      useStripe: () => ({ confirmCardPayment }),
      useElements: () => availability.elements === false
        ? null
        : { getElement: () => availability.card === false ? null : ({}) },
    },
  });
  function find(node, predicate) {
    if (!node || typeof node !== "object") return null;
    if (predicate(node)) return node;
    for (const child of React.Children.toArray(node.props?.children)) {
      const found = find(child, predicate);
      if (found) return found;
    }
    return null;
  }
  function render() {
    cursor = 0;
    const tree = StripePaymentForm({ clientSecret: "pi_fixture_secret", amount: 25, onSuccess, onError });
    const inner = React.Children.only(tree.props.children);
    return inner.type(inner.props);
  }
  function content(node) {
    if (typeof node === "string" || typeof node === "number") return String(node);
    if (!node || typeof node !== "object") return "";
    return React.Children.toArray(node.props?.children).map(content).join("");
  }
  const cardChange = (event) => find(render(), (node) => node.props?.id === "payment-card").props.onChange(event);
  cardChange({ complete: availability.complete !== false });
  return {
    cardChange,
    render,
    button: () => find(render(), (node) => node.type === "button"),
    text: () => content(render()),
    submit: () => render().props.onSubmit({ preventDefault() {} }),
  };
}

test("a successful card confirmation replaces Processing with a paid, non-retryable state", async () => {
  let succeeded = 0;
  let confirmation;
  const form = paymentForm(async (_clientSecret, params) => {
    confirmation = params;
    return { paymentIntent: { id: "pi_fixture", status: "succeeded" } };
  }, () => succeeded++);
  assert.match(JSON.stringify(form.button().props.children), /Pay \$25\.00/);

  await form.submit();

  const button = form.button();
  assert.ok(confirmation.payment_method.card);
  assert.equal(confirmation.payment_method.billing_details?.name, undefined);
  assert.equal(succeeded, 1);
  assert.equal(button.props.disabled, true);
  assert.match(JSON.stringify(button.props.children), /Payment received/);
  assert.doesNotMatch(JSON.stringify(button.props.children), /Processing/);
});

test("an incomplete card blocks button and programmatic submission until all fields are complete", async () => {
  let calls = 0;
  const form = paymentForm(async () => { calls++; return {}; }, undefined, undefined, { complete: false });
  assert.equal(form.button().props.disabled, true);
  await form.submit();
  assert.equal(calls, 0);
  assert.match(form.text(), /postal code/i);
  form.cardChange({ complete: true });
  assert.ok(!form.button().props.disabled);
  form.cardChange({ complete: false });
  await form.submit();
  assert.equal(calls, 0);
});

test("postal validation uses safe guidance instead of raw provider messages", async () => {
  const form = paymentForm(async () => ({ error: { type: "validation_error", code: "incomplete_zip", message: "secret provider payload" } }));
  await form.submit();
  assert.match(form.text(), /postal code/i);
  assert.doesNotMatch(form.text(), /secret provider payload/);
});

test("Chinese postal validation retains the specific translated guidance", async () => {
  const form = paymentForm(async () => ({ error: { type: "validation_error", code: "incomplete_zip", message: "secret payload" } }), undefined, undefined, { locale: "zh-CN" });
  await form.submit();
  assert.match(form.text(), /请检查账单邮编后重试/);
  assert.doesNotMatch(form.text(), /secret payload|无法确认付款/);
});

test("a declined card returns the button to a retryable Pay state", async () => {
  const form = paymentForm(async () => ({ error: { type: "card_error", message: "Your card was declined." } }));
  await form.submit();
  const button = form.button();
  assert.ok(!button.props.disabled);
  assert.match(JSON.stringify(button.props.children), /Pay \$25\.00/);
});

test("a non-final PaymentIntent status stays unconfirmed and blocks another confirmation", async () => {
  let succeeded = 0;
  const form = paymentForm(async () => ({ paymentIntent: { id: "pi_fixture", status: "processing" } }), () => succeeded++);

  await form.submit();

  assert.equal(succeeded, 0);
  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment is awaiting confirmation/);
  assert.doesNotMatch(form.text(), /Payment received/);
});

test("an explicit requires-payment-method result is a failed, retryable payment", async () => {
  let succeeded = 0;
  const form = paymentForm(async () => ({ paymentIntent: { id: "pi_fixture", status: "requires_payment_method" } }), () => succeeded++);

  await form.submit();

  assert.equal(succeeded, 0);
  assert.ok(!form.button().props.disabled);
  assert.match(form.text(), /Pay \$25\.00/);
  assert.doesNotMatch(form.text(), /Payment received/);
});

test("an SDK exception asks the customer to verify the order and prevents a repeat confirmation", async () => {
  let succeeded = 0;
  let errorMessage;
  const form = paymentForm(async () => { throw new Error("network failure"); }, () => succeeded++, (message) => { errorMessage = message; });

  await form.submit();

  assert.equal(succeeded, 0);
  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment status could not be confirmed/);
  assert.equal(errorMessage, undefined);
  assert.doesNotMatch(form.text(), /network failure|pi_fixture_secret/);
});

test("an empty Stripe response leaves confirmation blocked with a verification message", async () => {
  const form = paymentForm(async () => undefined);

  await form.submit();

  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment status could not be confirmed/);
  assert.doesNotMatch(form.text(), /Processing/);
});

test("two immediate submits make only one Stripe confirmation call", async () => {
  let calls = 0;
  let resolveConfirmation;
  let succeeded = 0;
  const form = paymentForm(() => {
    calls++;
    return new Promise((resolve) => { resolveConfirmation = resolve; });
  }, () => succeeded++);

  const first = form.submit();
  const second = form.submit();
  assert.equal(calls, 1);

  resolveConfirmation({ paymentIntent: { id: "pi_fixture", status: "succeeded" } });
  await Promise.all([first, second]);

  assert.equal(succeeded, 1);
  assert.equal(calls, 1);
  assert.equal(form.button().props.disabled, true);
});

test("an error carrying a succeeded PaymentIntent is reported as paid", async () => {
  let succeeded = 0;
  const form = paymentForm(async () => ({
    error: { type: "card_error", message: "stale SDK error", payment_intent: { id: "pi_fixture", status: "succeeded" } },
  }), (paymentIntentId) => { assert.equal(paymentIntentId, "pi_fixture"); succeeded++; });

  await form.submit();

  assert.equal(succeeded, 1);
  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment received/);
  assert.doesNotMatch(form.text(), /stale SDK error/);
});

test("an error carrying a non-retryable PaymentIntent status cannot re-enable Pay", async () => {
  let succeeded = 0;
  const form = paymentForm(async () => ({
    error: { type: "card_error", message: "stale SDK error", payment_intent: { id: "pi_fixture", status: "processing" } },
  }), () => succeeded++);

  await form.submit();

  assert.equal(succeeded, 0);
  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment is awaiting confirmation/);
});

test("a canceled PaymentIntent never reports success or offers confirmation again", async () => {
  let succeeded = 0;
  const form = paymentForm(async () => ({ paymentIntent: { id: "pi_fixture", status: "canceled" } }), () => succeeded++);

  await form.submit();

  assert.equal(succeeded, 0);
  assert.equal(form.button().props.disabled, true);
  assert.match(form.text(), /Payment status could not be confirmed/);
  assert.doesNotMatch(form.text(), /Payment received/);
});

test("missing Elements or CardElement shows a safe payment availability message", async () => {
  for (const availability of [{ elements: false }, { card: false }]) {
    const form = paymentForm(async () => { throw new Error("must not call Stripe"); }, () => {}, () => {}, availability);

    await form.submit();

    assert.match(form.text(), /Card payment is temporarily unavailable/);
    assert.doesNotMatch(form.text(), /must not call Stripe|pi_fixture_secret/);
    if (availability.elements === false) assert.equal(form.button().props.disabled, true);
  }
});
