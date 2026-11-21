"use client";

import { useRef, useState } from "react";
import { loadStripe } from "@stripe/stripe-js";
import { CardElement, Elements, useStripe, useElements } from "@stripe/react-stripe-js";
import type { StripeCardElement, StripeElementsOptions } from "@stripe/stripe-js";
import { useI18n } from "@/lib/i18n/index";

const publishableKey = process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY || "";
const stripePromise = publishableKey ? loadStripe(publishableKey) : null;

interface StripePaymentFormProps {
  clientSecret: string;
  amount: number;
  onSuccess: (paymentIntentId: string) => void;
  onError: (message: string) => void;
  disabled?: boolean;
}

function PaymentForm({ clientSecret, amount, onSuccess, onError, disabled }: StripePaymentFormProps) {
  const { t, usdCents } = useI18n();
  const stripe = useStripe();
  const elements = useElements();
  const [paymentState, setPaymentState] = useState<"idle" | "processing" | "paid" | "status-pending" | "verification-pending">("idle");
  const [errorKey, setErrorKey] = useState<"customer.payment.unavailable" | "customer.payment.completeCard" | "customer.payment.checkPostal" | "customer.payment.errorFallback" | null>(null);
  const [isFocused, setIsFocused] = useState(false);
  const [isInvalid, setIsInvalid] = useState(false);
  const [cardComplete, setCardComplete] = useState(false);
  const submissionInFlight = useRef(false);

  function styleCardElement(element: StripeCardElement) {
    const tokens = getComputedStyle(document.documentElement);
    element.update({
      style: {
        base: {
          fontSize: "16px",
          fontFamily: '-apple-system, BlinkMacSystemFont, "Segoe UI", Arial, sans-serif',
          color: tokens.getPropertyValue("--color-foreground").trim(),
          iconColor: tokens.getPropertyValue("--color-muted").trim(),
          "::placeholder": { color: tokens.getPropertyValue("--color-muted").trim() },
        },
        invalid: {
          color: tokens.getPropertyValue("--color-danger-ink").trim(),
          iconColor: tokens.getPropertyValue("--color-danger").trim(),
        },
      },
    });
  }

  function showPaymentUnavailable() {
    const key = "customer.payment.unavailable";
    const message = t(key);
    setErrorKey(key);
    onError(message);
  }

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();

    if (submissionInFlight.current || paymentState === "paid" || paymentState === "status-pending" || paymentState === "verification-pending" || disabled) {
      return;
    }

    if (!stripe || !elements) {
      showPaymentUnavailable();
      return;
    }

    const cardElement = elements.getElement(CardElement);
    if (!cardElement) {
      showPaymentUnavailable();
      return;
    }

    if (!cardComplete) {
      setErrorKey("customer.payment.completeCard");
      return;
    }

    submissionInFlight.current = true;
    setPaymentState("processing");
    setErrorKey(null);

    let result: Awaited<ReturnType<typeof stripe.confirmCardPayment>>;
    try {
      result = await stripe.confirmCardPayment(clientSecret, {
        payment_method: {
          card: cardElement,
          billing_details: {
            name: "TicketBox Customer",
          },
        },
      });
    } catch {
      setPaymentState("verification-pending");
      return;
    }

    const stripeError = result?.error;
    const paymentIntent = result?.paymentIntent ?? stripeError?.payment_intent;
    const safeErrorKey =
      stripeError?.code === "incomplete_zip" || stripeError?.code === "invalid_zip"
        ? "customer.payment.checkPostal"
        : stripeError?.code === "incomplete_number" || stripeError?.code === "incomplete_expiry" || stripeError?.code === "incomplete_cvc"
          ? "customer.payment.completeCard"
          : "customer.payment.errorFallback";
    const safeErrorMessage = t(safeErrorKey);

    if (paymentIntent?.status === "succeeded" && paymentIntent.id) {
      setPaymentState("paid");
      onSuccess(paymentIntent.id);
      return;
    }

    if (paymentIntent?.status === "requires_payment_method") {
      const message = safeErrorMessage;
      submissionInFlight.current = false;
      setPaymentState("idle");
      setErrorKey(safeErrorKey);
      onError(message);
      return;
    }

    if (paymentIntent) {
      const isAwaitingConfirmation = ["processing", "requires_action", "requires_confirmation", "requires_capture"].includes(paymentIntent.status);
      setPaymentState(isAwaitingConfirmation ? "status-pending" : "verification-pending");
      return;
    }

    if (stripeError?.type === "card_error" || stripeError?.type === "validation_error") {
      const message = safeErrorMessage;
      submissionInFlight.current = false;
      setPaymentState("idle");
      setErrorKey(safeErrorKey);
      onError(message);
      return;
    }

    setPaymentState("verification-pending");
  };

  const displayedError = errorKey ? t(errorKey) : null;

  return (
    <form onSubmit={handleSubmit} className="flex min-w-0 flex-col gap-4">
      <div className="flex flex-col gap-2">
        <label htmlFor="payment-card" className="text-sm font-medium">{t("customer.payment.cardDetails")}</label>
        <div className={`min-w-0 border rounded-lg p-4 bg-tag-bg transition-[border-color,box-shadow] ${isInvalid ? "border-danger" : isFocused ? "border-signal ring-2 ring-signal/10" : "border-control-border"}`}>
          <CardElement
            id="payment-card"
            onReady={styleCardElement}
            onFocus={() => setIsFocused(true)}
            onBlur={() => setIsFocused(false)}
            onChange={(event) => {
              setIsInvalid(Boolean(event.error));
              setCardComplete(event.complete);
              if (paymentState === "idle") setErrorKey(null);
            }}
          />
        </div>
      </div>

      {!cardComplete && paymentState === "idle" && (
        <p role="status" className="text-sm text-muted">{t("customer.payment.completeCard")}</p>
      )}

      {paymentState === "paid" && (
        <div role="status" className="bg-tag-bg text-sm p-3 rounded-lg">
          {t("customer.payment.confirmingBooking")}
        </div>
      )}

      {(paymentState === "status-pending" || paymentState === "verification-pending") && (
        <div role="status" className="bg-tag-bg text-sm p-3 rounded-lg">
          {paymentState === "status-pending"
            ? t("customer.payment.statusPending")
            : t("customer.payment.verificationPending")}
        </div>
      )}

      {displayedError && (
        <div className="min-w-0 bg-danger-bg text-danger-ink text-sm p-3 rounded-lg [overflow-wrap:anywhere]">
          {displayedError}
        </div>
      )}

      <div className="flex flex-wrap items-center justify-between gap-4 pt-4 border-t border-border">
        <div className="min-w-0 flex-1 [overflow-wrap:anywhere]">
          <span className="text-sm text-muted">{t("customer.payment.totalToPay")}</span>
          <span className="ml-2 text-2xl font-bold">{usdCents(Math.round(amount * 100))}</span>
        </div>
        <button
          type="submit"
          disabled={!stripe || !elements || !cardComplete || paymentState === "processing" || paymentState === "paid" || paymentState === "status-pending" || paymentState === "verification-pending" || disabled}
          className="flex min-w-0 max-w-full items-center justify-center gap-2 bg-primary hover:bg-primary-hover disabled:opacity-70 text-graphite font-semibold rounded-lg px-8 min-h-12 h-auto whitespace-normal text-center transition-colors [overflow-wrap:anywhere]"
        >
          {paymentState === "paid"
            ? t("customer.payment.paymentReceived")
            : paymentState === "processing"
              ? t("customer.payment.processing")
              : paymentState === "status-pending"
                ? t("customer.payment.verificationRequired")
                : paymentState === "verification-pending"
                  ? t("customer.payment.verificationRequired")
              : t("customer.payment.payAmount", { amount: usdCents(Math.round(amount * 100)) })}
        </button>
      </div>
    </form>
  );
}

export function StripePaymentForm({ clientSecret, amount, onSuccess, onError, disabled }: StripePaymentFormProps) {
  const { t } = useI18n();
  const options: StripeElementsOptions = {
    clientSecret,
  };

  // If publishable key is not provided, show an explanatory message instead of rendering Elements
  if (!publishableKey) {
    return (
      <div className="min-w-0 max-w-full p-4 bg-tag-bg border border-border rounded-lg">
        <p className="min-w-0 text-sm text-foreground [overflow-wrap:anywhere]">
          {t("customer.payment.unavailable")}
        </p>
      </div>
    );
  }

  return (
    <Elements stripe={stripePromise} options={options}>
      <PaymentForm
        clientSecret={clientSecret}
        amount={amount}
        onSuccess={onSuccess}
        onError={onError}
        disabled={disabled}
      />
    </Elements>
  );
}
