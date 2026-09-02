# FastAPI/services/calculator_factory.py

from margin_src.services import calculator as cambium_fn
from margin_src.Calculators.audio_array_ccb_engine import AudioArrayCCBCalculator


class CambiumCalculator:
    """
    Thin wrapper over existing Cambium calculate_single function.
    NO logic change.
    """

    def calculate_single(self, row, globals_):
        return cambium_fn.calculate_single(row, globals_)


class AudioArrayCBCalculator:
    """
    Audio Array CB calculator.

    FIX GUARANTEES:
    - SKU (Excel row) values ALWAYS win
    - globals_ act as fallback only
    - Behavior now matches Cambium
    """

    def __init__(self):
        self.engine = AudioArrayCCBCalculator()

    def calculate_single(self, row, globals_):
        """
        Merge strategy:
        - Start with globals_
        - Overlay SKU-level CB values from row (if present & non-empty)
        """

        # 1️⃣ Copy globals safely
        effective_globals = dict(globals_ or {})

        # 2️⃣ Overlay SKU-level values from Excel row
        if isinstance(row, dict):
            for key, value in row.items():
                if value not in (None, "", 0):
                    effective_globals[key] = value

        # 3️⃣ Pass merged data to engine
        return self.engine.calculate_single(row, effective_globals)


def get_calculator(calc_mode: str | None = None):
    """
    Central calculator selector.

    GUARANTEES:
    - Cambium logic unchanged
    - Audio Array CB is SKU-first
    """

    # --------------------------------------------------
    # SAFE NORMALIZATION
    # --------------------------------------------------
    if isinstance(calc_mode, str):
        calc_mode = calc_mode.strip().lower()
    else:
        calc_mode = ""

    # --------------------------------------------------
    # AUDIO ARRAY CB (SKU SAFE)
    # --------------------------------------------------
    if calc_mode == "audio_array_ccb":
        return AudioArrayCBCalculator()

    # --------------------------------------------------
    # DEFAULT: CAMBIUM (UNCHANGED)
    # --------------------------------------------------
    return CambiumCalculator()
