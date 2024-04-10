SELECT
    bill_BillingPeriodStartDate AS billing_period_start,
    bill_BillingPeriodEndDate AS billing_period_end,
    lineItem_UsageStartDate AS charge_period_start,
    lineItem_UsageEndDate AS charge_period_end,
    bill_PayerAccountId AS billing_account_id,
    lineItem_UsageAccountId AS sub_account_id,
    'TODO' AS billing_account_name,
    'TODO' AS charge_category,
    'TODO' AS charge_subcategory,
    lineItem_LineItemDescription AS charge_description,
    lineItem_ProductCode AS service_name,
    pricing_publicOnDemandRate AS list_unit_price,
    pricing_publicOnDemandCost AS list_cost,
    COALESCE(lineItem_NetUnblendedCost, lineItem_UnblendedCost) AS billed_cost,
    CASE
        WHEN lineItem_LineItemType = 'DiscountedUsage'
            THEN COALESCE(
                reservation_NetEffectiveCost, reservation_EffectiveCost
            )
        WHEN
            lineItem_LineItemType = 'SavingsPlanCoveredUsage'
            THEN COALESCE(
                savingsPlan_NetSavingsPlanEffectiveCost, savingsPlan_SavingsPlanEffectiveCost
            )
        WHEN
            lineItem_LineItemType = 'RIFee'
            THEN COALESCE(
                reservation_NetUnusedAmortizedUpfrontFeeForBillingPeriod,
                reservation_UnusedAmortizedUpfrontFeeForBillingPeriod, 0
            ) + COALESCE(reservation_NetUnusedRecurringFee, reservation_UnusedRecurringFee, 0)
        WHEN (lineItem_LineItemType = 'Fee' AND reservation_ReservationARN <> '') THEN 0
        WHEN
            lineItem_LineItemType = 'SavingsPlanRecurringFee'
            THEN COALESCE(
                savingsPlan_TotalCommitmentToDate, 0
            ) - COALESCE(savingsPlan_UsedCommitment, 0)
        WHEN lineItem_LineItemType IN ('SavingsPlanNegation', 'SavingsPlanUpfrontFee') THEN 0
        WHEN
            lineItem_LineItemType IN (
                'Usage', 'Tax', 'Credit', 'Refund'
            ) THEN COALESCE(lineItem_NetUnblendedCost, lineItem_UnblendedCost)
        ELSE 0
    END AS effective_cost

FROM {{ ref('base_aws_v1') }}
