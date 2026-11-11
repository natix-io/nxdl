class UserExceptions(Exception):
    pass

class NotSupportedTripFormat(UserExceptions):
    def __init__(self, format, file_type):
        super().__init__(f"Trip file format {format} is not supported for generating {file_type}.")

class NotSupportedFootageFormat(UserExceptions):
    def __init__(self, format, file_type):
        super().__init__(f"Footage file format {format} is not supported for generating {file_type}.")