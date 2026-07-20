PUBLIC_EVENT_NAME_TO_INTERNAL_NAMES = {
    "harsh_brake": ("brake_harsh", "brake_severe"),
    "harsh_accel": ("accel_harsh", "accel_severe"),
    "harsh_turn": ("turn_harsh", "turn_severe"),
    "aggressive_drive": ("aggressive_driving",),
    "severe_speeding": ("speeding_severe",),
    "crash": ("crash",),
}


def internal_names_for_public_names(public_names: list[str]) -> set[str]:
    names: set[str] = set()
    for public_name in public_names:
        names.update(PUBLIC_EVENT_NAME_TO_INTERNAL_NAMES.get(public_name, ()))
    return names


def public_name_for_internal(internal_name: str) -> str | None:
    for public_name, internal_names in PUBLIC_EVENT_NAME_TO_INTERNAL_NAMES.items():
        if internal_name in internal_names:
            return public_name
    return None
