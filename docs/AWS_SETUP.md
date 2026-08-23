# AWS Free Plan — Account Setup Guide

This is the step-by-step walkthrough for Week 0: creating the single AWS account that
both `retail-lakehouse-aws` and `data-perf-teardown` run on. Do all of this before
creating a single S3 bucket or writing a line of Terraform against real AWS.

Background on *why* these choices (Free Plan vs Paid, budget math, architecture
implications) lives in this project's execution plan; this doc is the *how*.

---

## 0. What you're signing up for

AWS restructured its free tier in July 2025 into a credit model. Every new account —
Free or Paid — gets:

- **$100 in credits** immediately on signup
- **Up to $100 more** by completing 5 onboarding activities ($20 each)
- Access to 30+ **Always Free** services with permanent monthly usage limits, on top of the credits

The two plans diverge like this:

| | Free Plan | Paid Plan |
|---|---|---|
| Can you be billed past your credits? | **No — the account just stops working** | Yes, standard pay-as-you-go |
| Service access | Select services only | All services + short-term trials |
| Account lifetime | 6 months or credits exhausted, whichever first | Indefinite |
| Other promo credits | Not eligible | Eligible |

**Choose Free Plan.** For a first real AWS account, "cannot be billed" beats "full service
catalog" — the two projects here don't need anything outside the Free Plan's service list.

**Grace period if you run out of time:** if your Free Plan account closes, AWS keeps your
data for **90 days** before permanent deletion, during which you can upgrade to Paid and
resume with any leftover credit intact. Don't rely on this — the design rule below (§5)
is to need it as little as possible — but know it exists as a backstop.

Sources: [AWS Billing docs — choosing a plan](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/free-tier-plans.md), [AWS announcement, July 2025](https://aws.amazon.com/about-aws/whats-new/2025/07/aws-free-tier-credits-month-free-plan/)

---

## 1. Sign up

1. Go to `aws.amazon.com` → **Create an AWS Account**.
2. Use an email you don't mind tying to a long-lived portfolio account (not a throwaway —
   you'll want to log back in during interview prep months later).
3. Account name: something identifiable, e.g. `garima-de-portfolio`.
4. Choose **Personal** account type.
5. Enter a payment card. This is required even for the Free Plan (identity verification) —
   it does not mean you can be charged past your credit balance; that's the whole point of
   the plan.
6. Complete phone verification.
7. **When the plan selection screen appears, choose Free Plan, not Paid Plan.** This is a
   genuinely separate choice in the current signup flow, not just a default — read the
   screen, don't click through it.
8. Support plan: **Basic** (free). You don't need paid support for this.

## 2. Confirm you're actually on the Free Plan

Signup flows change. Don't assume — verify:

1. Sign in to the [Billing and Cost Management console](https://console.aws.amazon.com/costmanagement/).
2. Look for your plan type and credit balance on the home page.
3. Confirm it says **Free plan**, not Paid.

## 3. Lock down the account before touching any service

1. **MFA on the root user immediately.** IAM → root user → enable MFA (an authenticator
   app is fine). Do this before anything else — the root user has no permission
   boundaries.
2. **Create an IAM user for yourself** with `AdministratorAccess` (fine for a solo
   portfolio account) and **use this for everything from now on**. Never sign in as root
   again except for account-level changes MFA itself requires.
3. **Generate an access key for that IAM user** — Terraform needs it. Store it via
   `aws configure` locally (creates `~/.aws/credentials`), never commit it to Git. Both
   repos' `.gitignore` already exclude `.env` and AWS credential files — double check
   before your first commit that touches infra code.
4. **Set your default region to `ap-south-1` (Mumbai)** — lower latency from Bengaluru,
   and keeping everything in one region avoids cross-region data transfer charges.
   Set it in `~/.aws/config` and hardcode it in every Terraform provider block; don't
   let any resource default to `us-east-1`.

## 4. Earn the second $100 — do this in week 1, not "eventually"

Five onboarding activities, $20 credit each, roughly an hour total. This is the single
most commonly-skipped step people regret:

1. Launch an EC2 instance, then terminate it.
2. Configure an RDS database (you can delete it right after).
3. Deploy a Lambda function (a "hello world" is enough).
4. Test a prompt in Amazon Bedrock.
5. Set up a cost budget in AWS Budgets — do this one first, since you need the budget
   alarms below anyway and it kills two birds at once.

Check the Billing console's credits page to confirm all five landed before moving on.

## 5. Budget alarms — three of them, before any pipeline resource exists

AWS Budgets → Create budget → Cost budget:

| Threshold | Alert action |
|---|---|
| $25 | Email: "checkpoint, review burn rate" |
| $75 | Email: "over half gone, stop and audit" |
| $150 | Email: "emergency, tear everything down" |

All three route to your email. This also counts as onboarding activity #5 above.

## 6. Verify the services you actually need are on the Free Plan

Free Plan access is "select services," not everything. Before scaffolding either
project's Terraform, open the [AWS Free Tier page](https://aws.amazon.com/free/) from
inside your console and confirm these are all listed as available to you:

- S3
- AWS Glue (Data Catalog + Crawler)
- Athena
- EMR Serverless
- Kinesis Data Streams
- Redshift Serverless
- Lambda
- DynamoDB
- CloudWatch, SNS, Budgets, IAM

**If Redshift Serverless is not available on your Free Plan:** don't improvise — this
changes exactly one experiment (`data-perf-teardown` E4). Fall back to ClickHouse or
Postgres on an EC2 `t3.micro` (always-free tier) to demonstrate sort-order/clustering-key
effects instead, and document the substitution honestly in that project's README. Every
other part of both projects is unaffected.

## 7. Write `make teardown` before you write any pipeline code

In `retail-lakehouse-aws/Makefile` (create it now, even before Terraform exists):

```make
teardown:
	aws redshift-serverless delete-workgroup --workgroup-name perf-wg || true
	aws redshift-serverless delete-namespace --namespace-name perf-ns || true
	aws kinesis delete-stream --stream-name order-events || true
	aws emr-serverless list-applications --query 'applications[].id' \
	  --output text | xargs -n1 -I{} aws emr-serverless stop-application --application-id {} || true
	@echo "Teardown complete. Verify in Cost Explorer tomorrow."
```

Run this at the end of **every** working session, not most of them. The two costliest
mistakes in this whole plan are a Redshift Serverless workgroup or a Kinesis stream left
running between sessions — both are cheap per-hour and expensive per-forgotten-week.

## 8. S3 lifecycle rules, day one

Even before real data lands:

- Raw zone → transition to Glacier Instant Retrieval after 30 days.
- Delete incomplete multipart uploads after 7 days (these accumulate silently from
  interrupted `aws s3 sync` runs and you pay for them without ever seeing them in the
  console's normal object listing).

## 9. Weekly habit

Cost Explorer, every Monday morning, two minutes. Look for anything running that
shouldn't be — this is how a forgotten NAT Gateway or an un-torn-down Redshift workgroup
gets caught in days instead of weeks.

## 10. Never do these — they void your Free Plan credits immediately

Joining an **AWS Organization**, setting up **AWS Control Tower**, joining the **AWS
Partner Network**, signing a **Professional Services contract**, enrolling in an
**Enterprise Agreement**, purchasing an **AWS Skill Builder Team subscription**, or
designating the account as **HIPAA/SEC compliant** — every one of these silently
auto-upgrades you to a Paid Plan and forfeits Free Plan treatment. None of them are
things this project needs; the danger is clicking into one by accident while exploring
the console.

## 11. When the account is nearing expiry (month 4-5)

- **Month 4**: finish all AWS-dependent work. New experimentation stops here.
- **Month 5**: export everything — Terraform state, Glue Catalog DDL
  (`infra/catalog/*.sql`), all screenshots and result CSVs — and verify both repos are
  standalone (a fresh clone + `terraform apply` against a *new* account should
  reproduce everything).
- **Month 5.5**: decide — let the account close (fine if both repos are genuinely
  self-contained), or upgrade to Paid Plan with one click if you want a live environment
  for interview demos. Remaining credits carry over and expire 12 months from original
  signup. If you upgrade, keep the three Budget alarms active — overage on a Paid Plan
  is a real charge.

---

**This file is duplicated in `data-perf-teardown/docs/AWS_SETUP.md`** since both projects
share one account — do Week 0 once, and it covers both repos.
