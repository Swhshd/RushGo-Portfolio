import assert from 'node:assert/strict';
import { test } from 'node:test';
import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { loadTypeScript } from './render-typescript.mjs';

const { awaitingRefundApproval, unapprovedRefundNotice, refundDecisionReason } = loadTypeScript('lib/refund-presentation.ts');
const { buyerRefundEntryAccess } = loadTypeScript('lib/booking-refunds.ts');
const { getI18n } = loadTypeScript('lib/i18n/core.ts');
const rejected = { status:'REJECTED',payment_refund_state:'unknown',ticket_revocation_state:'NOT_STARTED',
  decision_reason:'ticket_set_mismatch',amount_cents:100,currency:'USD',can_withdraw:false };
const requested = {...rejected,status:'REQUESTED',decision_reason:'',refund_decision_id:'',decided_at:'',
  request_id:'request-1',booking_id:'booking-1',requested_at:'2026-11-06T12:00:00Z',reason:'Change of plans'};

test('only an undecided request without downstream work gets pre-approval copy', () => {
  assert.equal(awaitingRefundApproval(requested),true);
  assert.equal(buyerRefundEntryAccess(true,requested),'BLOCKED');
  for(const changed of [
    {refund_decision_id:'decision-1'},{refund_decision_id:undefined},{decided_at:'2026-11-06T12:05:00Z'},
    {payment_refund_state:'pending'},{payment_refund_state:'succeeded'},{payment_refund_state:'new_state'},
    {ticket_revocation_state:'PENDING'},{ticket_revocation_state:'REVOKED'},{ticket_revocation_state:'NEEDS_ATTENTION'},
    {ticket_revocation_state:''},{ticket_revocation_state:'new_state'},
    {status:'APPROVED'},{status:'REFUND_PROCESSING'},{status:'REFUNDED'},{status:'NEEDS_ATTENTION'},
    {status:'WAITING_CHECKIN_RESOLUTION'},{status:'new_status'},
  ]) assert.equal(awaitingRefundApproval({...requested,...changed}),false,JSON.stringify(changed));
});

test('aftersales initial server markup is stable before browser session hydration', () => {
  const render=isLoading=>{
    const {default:Page}=loadTypeScript('app/(customer)/aftersales/page.tsx',{
      '@/lib/auth-context':{useAuth:()=>({user:null,isLoading})},
      '@/lib/i18n':{useI18n:()=>getI18n('en-US')},
      'next/navigation':{useRouter:()=>({push:()=>{}})},
      'next/link':{default:({children,...props})=>React.createElement('a',props,children)}
    });
    return renderToStaticMarkup(React.createElement(Page));
  };
  assert.equal(render(false),render(true));
  assert.ok(render(false).includes(getI18n('en-US').t('customer.wallet.sessionChecking')));
});

test('declined copy does not change the conservative admission gate', () => {
  assert.equal(unapprovedRefundNotice(rejected),'aftersales.notApproved');
  assert.equal(buyerRefundEntryAccess(true,rejected),'BLOCKED');
  for(const state of ['APPROVED','REFUND_PROCESSING','REFUNDED','NEEDS_ATTENTION','REQUESTED','NEW_STATE'])
    assert.equal(unapprovedRefundNotice({...rejected,status:state}),null);
  for(const payment of ['pending','succeeded','unexpected'])
    assert.equal(unapprovedRefundNotice({...rejected,payment_refund_state:payment}),null);
  for(const ticket of ['PENDING','REVOKED','NEEDS_ATTENTION','unexpected'])
    assert.equal(unapprovedRefundNotice({...rejected,ticket_revocation_state:ticket}),null);
});

for(const locale of ['en-US','zh-CN']) test(`real refund panel explains rejection and preserves active warnings (${locale})`, () => {
  const i18n=getI18n(locale);
  function render(refund) {
    let index=0;
    const slots=[refund,'',true,false,false,false,false,false,'',null];
    const hooks={...React,useState:()=>[slots[index++],()=>{}],useEffect:()=>{},useCallback:fn=>fn,useRef:v=>({current:v})};
    const { WalletRefundPanel }=loadTypeScript('components/wallet-refund-panel.tsx',{
      react:hooks,'@/lib/i18n/index':{useI18n:()=>i18n},'@/lib/api/client':{apiClient:{}}
    });
    return renderToStaticMarkup(React.createElement(WalletRefundPanel,{userId:'buyer',bookingId:'booking',bookingStatus:'CONFIRMED',amountCents:100,hasIssuedTicket:false,hasCheckedInTicket:true,onRefundAccessChange:()=>{}}));
  }
  const html=render(rejected);
  assert.ok(html.includes(i18n.t('aftersales.notApproved')));
  assert.ok(html.includes(i18n.t('aftersales.ticketNotEligible')));
  assert.ok(!html.includes('ticket_set_mismatch'));
  assert.ok(!html.includes(i18n.t('customer.refund.paymentUnknownExplanation')));
  const active=render({...rejected,status:'APPROVED',ticket_revocation_state:'PENDING'});
  assert.ok(active.includes(i18n.t('customer.refund.paymentUnknownExplanation')));
  assert.ok(!active.includes(i18n.t('aftersales.notApproved')));
  const waiting=render(requested);
  assert.ok(waiting.includes(i18n.t('aftersales.awaitingApproval')));
  assert.ok(!waiting.includes(i18n.t('customer.refund.paymentUnknownExplanation')));
  const inconsistent=render({...requested,ticket_revocation_state:'PENDING'});
  assert.ok(inconsistent.includes(i18n.t('customer.refund.paymentUnknownExplanation')));
  assert.equal(refundDecisionReason('Reviewed unused order',i18n.t),'Reviewed unused order');
  assert.equal(refundDecisionReason('unrecognized_machine_reason',i18n.t),i18n.t('aftersales.reasonUnavailable'));
});

for(const locale of ['en-US','zh-CN']) test(`real approval summary distinguishes waiting from follow-up (${locale})`, () => {
  const i18n=getI18n(locale);
  const render=refund=>{
    let index=0;
    const slots=[{refunds:[refund],approval_enabled:true,approval_block_reason:'',next_cursor:''},[],'',{},false,false,'','','','',null,{}];
    const hooks={...React,useState:()=>[slots[index++],()=>{}],useEffect:()=>{},useCallback:fn=>fn,useRef:v=>({current:v})};
    const {AdminEventRefunds}=loadTypeScript('components/admin-event-refunds.tsx',{
      react:hooks,'@/lib/i18n/index':{useI18n:()=>i18n},'@/lib/api/client':{apiClient:{}}
    });
    return renderToStaticMarkup(React.createElement(AdminEventRefunds,{eventId:'event',userId:'approver'}));
  };
  const summary=html=>html.match(new RegExp('<div class="([^"]*)"><dt[^>]*>'+i18n.t('admin.refunds.summaryFollowUp')+'</dt><dd[^>]*>([0-9]+)</dd></div>'));
  const waiting=render(requested);
  assert.ok(waiting.includes(i18n.t('aftersales.awaitingApproval')));
  assert.ok(waiting.includes(i18n.t('aftersales.paymentNotStarted')));
  assert.ok(!waiting.includes(i18n.t('admin.refunds.paymentUnknownExplanation')));
  assert.equal(summary(waiting)?.[2],'0');
  assert.ok(!summary(waiting)?.[1].includes('danger'));
  for(const changed of [{status:'APPROVED'},{status:'NEEDS_ATTENTION'},{status:'WAITING_CHECKIN_RESOLUTION'},
    {refund_decision_id:'decision-1'},{payment_refund_state:'pending'},{payment_refund_state:'succeeded'},
    {ticket_revocation_state:'PENDING'},{ticket_revocation_state:'REVOKED'},{ticket_revocation_state:'NEEDS_ATTENTION'},
    {payment_refund_state:'future'},{status:'future'}]) {
    const markup=render({...requested,...changed});
    assert.equal(summary(markup)?.[2],'1',JSON.stringify(changed));
    assert.ok(summary(markup)?.[1].includes('danger'));
    assert.ok(!markup.includes(i18n.t('aftersales.awaitingApproval')));
  }
});

test('collapsed order card separates booking status from the authoritative refund result', () => {
  const i18n=getI18n('en-US');
  let panelUpdate='loading';
  let summaryState='loading';
  const hooks={...React,useState:initial=>{
    const state=React.useState(initial);
    return initial==='loading'
      ? [summaryState,value=>{summaryState=typeof value==='function'?value(summaryState):value}]
      : state;
  }};
  function WalletRefundPanelMock({onRefundSummaryChange}) {
    onRefundSummaryChange(panelUpdate);
    return null;
  }
  const {WalletOrderCard}=loadTypeScript('components/wallet-order-card.tsx',{
    react:hooks,
    '@/components/wallet-refund-panel':{WalletRefundPanel:WalletRefundPanelMock},
    '@/lib/i18n/index':{useI18n:()=>i18n},
  });
  const order={id:'booking-1',eventId:'event-1',status:'CONFIRMED',rawStatus:'CONFIRMED',statusLabel:'Confirmed',
    statusTone:'success',isConfirmedBooking:true,totalCents:1000,createdAt:'2026-11-08T10:00:00Z',quantity:1,
    seatIds:[],items:[],event:null};
  const render=()=>renderToStaticMarkup(React.createElement(WalletOrderCard,{
    userId:'buyer-1',order,expanded:false,onToggle(){},hasIssuedTicket:false,hasCheckedInTicket:false,
    onRefundAccessChange(){},
  }));
  const renderAfterUpdate=update=>{
    panelUpdate=update;
    summaryState='loading';
    render(); // The child callback updates the card summary.
    return render();
  };

  const loading=renderAfterUpdate('loading');
  assert.ok(loading.includes(i18n.t('aftersales.refundStatusLoading')));
  assert.ok(!loading.includes(i18n.t('aftersales.refundComplete')));
  assert.ok(loading.includes('Booking status'));
  assert.ok(loading.includes('Confirmed'));

  const complete=renderAfterUpdate({...requested,status:'REFUNDED',payment_refund_state:'succeeded',ticket_revocation_state:'REVOKED'});
  assert.ok(complete.includes(i18n.t('aftersales.refundComplete')));
  assert.ok(complete.includes(i18n.t('aftersales.refundLabel',{status:i18n.t('aftersales.refundComplete')})));

  const revocationPending=renderAfterUpdate({...requested,status:'REFUNDED',payment_refund_state:'succeeded',ticket_revocation_state:'PENDING'});
  assert.ok(revocationPending.includes(i18n.t('aftersales.refundTicketRevocationPending')));
  assert.ok(!revocationPending.includes(i18n.t('aftersales.refundComplete')));

  const error=renderAfterUpdate('error');
  assert.ok(error.includes(i18n.t('aftersales.refundStatusUnavailable')));
  assert.ok(!error.includes(i18n.t('aftersales.refundComplete')));
});
