from .calibrations import calibrations_data


def get_camera_and_device_calibrations(
    hw_version: str,
    model: str,
) -> tuple[list[dict], list[dict]]:
    """Return camera/device calibrations for a vehicle setup."""

    error_message = (
        f"Calibration data not found for hw_version='{hw_version}' and "
        f"model='{model}'."
    )

    if hw_version not in calibrations_data:
        raise ValueError(error_message)

    model_calibrations = calibrations_data[hw_version]
    if model not in model_calibrations:
        raise ValueError(error_message)

    selected_calibrations = model_calibrations[model]
    if "camara" not in selected_calibrations or "device" not in selected_calibrations:
        raise ValueError(error_message)

    return selected_calibrations["camara"], selected_calibrations["device"]
