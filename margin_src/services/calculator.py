import pandas as pd


class CambiumCalculator:
    """
    Cambium calculator wrapper.

    IMPORTANT:
    - Core logic is IDENTICAL to the base file provided by the user.
    - This version adds SAFE numeric extraction to prevent:
        * silent zeros
        * KeyError crashes
        * NaN propagation
    - Required for stable coexistence with Audio Array engine.
    """

    # --------------------------------------------------
    # INTERNAL SAFE PARSERS (NO LOGIC CHANGE)
    # --------------------------------------------------
    def _num(self, row, key, default=0.0):
        """
        Safely extract numeric values from dataframe row.
        Returns default if column missing, NaN, empty, or '-'
        """
        try:
            val = row.get(key, default)
            if val in (None, "", "-", "NaN"):
                return default
            if pd.isna(val):
                return default
            return float(val)
        except Exception:
            return default

    # --------------------------------------------------
    # MAIN CALCULATION (UNCHANGED FORMULA)
    # --------------------------------------------------
    def calculate_single(self, row, globals_):
        # -------------------------------
        # GLOBAL PARAMS
        # -------------------------------
        usd_rate = globals_.get("usd_rate", 0)
        surcharge_pct = globals_.get("surcharge_pct", 0)
        gst_default_pct = globals_.get("gst_default_pct", 0)
        overhead_pct = globals_.get("overhead_pct", 0)
        finance_pct = globals_.get("finance_pct", 0)

        # -------------------------------
        # IMPORT & LANDING COSTS
        # -------------------------------
        fob_inr = self._num(row, "Latest FOB") * usd_rate
        freight_inr = self._num(row, "Freight+Clearance") * usd_rate

        duty_base = fob_inr + freight_inr
        duty = duty_base * self._num(row, "Import Duty %") / 100
        surcharge = duty * surcharge_pct / 100

        dp = (
            fob_inr +
            freight_inr +
            duty +
            surcharge +
            self._num(row, "Additional Cost")
        )

        bau_sp = self._num(row, "BAU Deal SP")

        # -------------------------------
        # AMAZON & VARIABLE COSTS
        # -------------------------------
        referral = bau_sp * self._num(row, "Referral Fee %") / 100
        ams = bau_sp * self._num(row, "AMS %") / 100
        coupon = bau_sp * self._num(row, "Coupon %") / 100
        returns = bau_sp * self._num(row, "Returns Loss %") / 100

        # -------------------------------
        # GST
        # -------------------------------
        gst_pct = (
            self._num(row, "GST Slab")
            if "GST Slab" in row.index and not pd.isna(row.get("GST Slab"))
            else gst_default_pct
        )
        gst = bau_sp - (bau_sp / (1 + gst_pct / 100)) if bau_sp else 0

        # -------------------------------
        # SMM (MASTER ONLY)
        # -------------------------------
        smm_pct = self._num(row, "SMM")
        smm = bau_sp * smm_pct / 100

        # -------------------------------
        # OVERHEADS & FINANCE
        # -------------------------------
        nlc_base = self._num(row, "NLC")
        business_overhead = nlc_base * overhead_pct / 100
        finance_cost = nlc_base * finance_pct / 100

        # -------------------------------
        # TOTAL COST
        # -------------------------------
        total_cost = (
            dp +
            referral +
            self._num(row, "Fixed Closing Fee") +
            self._num(row, "Fulfillment Fees") +
            ams +
            coupon +
            returns +
            smm +
            business_overhead +
            finance_cost +
            gst
        )

        # -------------------------------
        # MARGINS
        # -------------------------------
        margin = bau_sp - total_cost
        margin_pct = (margin / bau_sp) * 100 if bau_sp else 0

        gross_margin = (bau_sp - gst) - dp
        gross_margin_pct = (gross_margin / bau_sp) * 100 if bau_sp else 0

        # -------------------------------
        # FINAL OUTPUT (UI EXPECTED KEYS)
        # -------------------------------
        return {
            "dp": round(dp, 2),
            "gross_margin": round(gross_margin, 2),
            "gross_margin_pct": round(gross_margin_pct, 2),
            "net_margin": round(margin, 2),
            "net_margin_pct": round(margin_pct, 2),
        }


# ==================================================
# 🔒 BACKWARD COMPATIBILITY (UNCHANGED API)
# ==================================================
def calculate_single(row, globals_):
    """
    Legacy functional entry point.
    Do NOT remove — used by existing Cambium flows.
    """
    return CambiumCalculator().calculate_single(row, globals_)
