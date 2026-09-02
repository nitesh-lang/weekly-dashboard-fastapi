# weekly_app/calculators/schemas/audio_array_ccb_schema.py

SCHEMA = {
    "ccb_status": {
        "label": "CCB Status",
        "source": "excel",
        "sheet": "Main",
        "column": "CCB Status",
        "editable": False,
        "filter": True
    },

    "ccb_sp": {
        "label": "CCB SP (₹)",
        "source": "excel",
        "sheet": "Main",
        "column": "CCB SP",
        "editable": True
    },

    "fee_structure_pct": {
        "label": "Fee Structure %",
        "source": "excel",
        "sheet": "Main",
        "column": "Fee Structure %",
        "editable": True
    },

    "promo_waiver_pct": {
        "label": "Promotional Waiver %",
        "source": "excel",
        "sheet": "Main",
        "column": "Promotional Waiver on Fee Structure",
        "editable": True
    },

    "ams_pct": {
        "label": "AMS %",
        "source": "excel",
        "sheet": "Main",
        "column": "AMS",
        "editable": True
    },

    "business_overhead_pct": {
        "label": "Business Overhead %",
        "source": "excel",
        "sheet": "Main",
        "column": "Business Overhead",
        "editable": True
    },

    "finance_cost_pct": {
        "label": "Finance Cost %",
        "source": "excel",
        "sheet": "Main",
        "column": "Finance Cost",
        "editable": True
    },

    "coupon_pct": {
        "label": "Coupon %",
        "source": "excel",
        "sheet": "Main",
        "column": "Coupon",
        "editable": True
    }
}
