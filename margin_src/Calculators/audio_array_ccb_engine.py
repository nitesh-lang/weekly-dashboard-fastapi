
# ======================================================
# Audio Array CCB Calculator Engine
# ======================================================
# PURPOSE:
#   - Dedicated calculator for Audio Array CCB mode
#   - Uses GLOBAL parameters (from UI / Main sheet row-1)
#   - Excel row values are fallback only
#
# GUARANTEES:
#   - Cambium calculator untouched
#   - Math logic unchanged
#   - Deterministic, stateless per call
#
# NOTE:
#   This file intentionally verbose for auditability
# ======================================================


class AudioArrayCCBCalculator:
    """
    Audio Array CCB Margin Calculator
    --------------------------------
    Entry point used via calculator_factory.get_calculator("audio_array_ccb")

    Flow:
      main.py
        → get_calculator()
        → calculate_single(row, global_params)
        → calculate(payload)

    All UI edits MUST reflect via `globals_`
    """

    # --------------------
    # CONSTANTS
    # --------------------
    GST_RATE = 0.18

    # ======================================================
    # PUBLIC API
    # ======================================================
    def calculate_single(self, row, globals_):
        """
        Adapter to align with Cambium engine signature.

        Priority order (CRITICAL):
        1. globals_  -> UI / Main sheet row-1 (CB Edit Parameters)
        2. row       -> SKU-level Excel fallback
        3. default   -> 0

        NO business math is changed here.
        """

        # --------------------
        # SAFE NUMERIC CAST
        # --------------------
        def _num(v):
            try:
                if v in ("", None, "-", "--"):
                    return 0.0
                return float(v)
            except Exception:
                return 0.0

        # --------------------
        # PAYLOAD ASSEMBLY
        # --------------------
        payload = {
            "ccb_sp": _num(globals_.get("ccb_sp", row.get("CCB SP", 0))),

            "fee_structure_pct": _num(
                globals_.get("fee_structure_pct", row.get("Fee Structure %", row.get("Fee Structure", 0)))
            ),
            "promo_waiver_pct": _num(
                globals_.get("promo_waiver_pct", row.get("Promotional Waiver on Fee Structure", 0))
            ),

            "ams_pct": _num(
                globals_.get("ams_pct", row.get("AMS", row.get("AMS %", 0)))
            ),
            "business_overhead_pct": _num(
                globals_.get("business_overhead_pct", row.get("Business Overhead", row.get("Business Overhead %", 0)))
            ),
            "finance_cost_pct": _num(
                globals_.get("finance_cost_pct", row.get("Finance Cost", row.get("Finance Cost %", 0)))
            ),
            "coupon_pct": _num(
                globals_.get("coupon_pct", row.get("Coupon", row.get("Coupon %", 0)))
            ),

            "cambium_dp": _num(row.get("Cambium DP", 0)),
        }

        return self.calculate(payload)

    # ======================================================
    # CORE CALCULATION (PURE FUNCTION)
    # ======================================================
    def calculate(self, payload: dict) -> dict:
        """
        Core CCB calculation.
        Input: normalized payload (all numeric)
        Output: dict compatible with Cambium UI
        """

        ccb_sp = payload.get("ccb_sp", 0)

        fee_structure_pct = payload.get("fee_structure_pct", 0) / 100
        promo_waiver_pct = payload.get("promo_waiver_pct", 0) / 100

        ams_pct = payload.get("ams_pct", 0) / 100
        business_overhead_pct = payload.get("business_overhead_pct", 0) / 100
        finance_cost_pct = payload.get("finance_cost_pct", 0) / 100
        coupon_pct = payload.get("coupon_pct", 0) / 100

        cambium_dp = payload.get("cambium_dp", 0)

        adjusted_referral_pct = fee_structure_pct - promo_waiver_pct

        ccb_nlc = ccb_sp * (1 - adjusted_referral_pct)
        ccb_dp = ccb_nlc / (1 + self.GST_RATE)

        cambium_nlc = cambium_dp * (1 + self.GST_RATE)

        gross_margin = ccb_dp - cambium_dp
        gross_margin_nlc = ccb_nlc - cambium_nlc
        gross_margin_pct = (gross_margin / ccb_dp) if ccb_dp else 0

        ams = ams_pct * ccb_sp
        business_overhead = business_overhead_pct * ccb_sp
        finance_cost = finance_cost_pct * ccb_sp
        coupon = coupon_pct * ccb_sp

        total_additional_expenses = (
            ams + business_overhead + finance_cost + coupon
        ) * (1 + self.GST_RATE)

        net_margin = gross_margin - total_additional_expenses
        net_margin_pct = (net_margin / ccb_dp) if ccb_dp else 0

        return {
            "ccb_sp": ccb_sp,
            "adjusted_referral_pct": adjusted_referral_pct * 100,

            "ccb_nlc": ccb_nlc,
            "ccb_dp": ccb_dp,

            "cambium_dp": cambium_dp,
            "cambium_nlc": cambium_nlc,

            "gross_margin": gross_margin,
            "gross_margin_nlc": gross_margin_nlc,
            "gross_margin_pct": gross_margin_pct * 100,

            "ams": ams,
            "business_overhead": business_overhead,
            "finance_cost": finance_cost,
            "coupon": coupon,

            "total_additional_expenses": total_additional_expenses,

            "net_margin": net_margin,
            "net_margin_pct": net_margin_pct * 100,

            "dp": ccb_dp,
            "net_margin_value": net_margin,
        }


# ======================================================
# END OF FILE
# ======================================================
# ======================================================
# ======================================================
# ======================================================
# ======================================================


 
 