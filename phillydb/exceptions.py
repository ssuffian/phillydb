class SearchTypeNotImplementedError(Exception):
    """Exception raised for errors where search type provided isn't implemented."""

    def __init__(self, search_type): 
        self.message = (
            f"Invalid search_type: '{search_type}' provided. Must be one of: ('owner', "
            "'mailing_address', 'location_by_mailing_address', 'location_by_owner')"
        )
        super().__init__(self.message)


class SearchMethodNotImplementedError(Exception):
    """Exception raised for errors where search method provided isn't implemented."""

    def __init__(self, search_method): 
        self.message = (
            f"Invalid search_method: '{search_method}' provided. Must be one of: "
            "('contains', 'starts_with', 'ends_with')"
        )
        super().__init__(self.message)
