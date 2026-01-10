"""
OpenCorporates API Client Module

Provides access to corporate registry data from OpenCorporates,
the world's largest open database of companies with 200M+ records.

API Documentation: https://api.opencorporates.com/documentation/API-Reference

Rate Limits:
- Free tier: 500 requests/month
- Authenticated: Higher limits based on plan

Usage:
    from src.ingest.opencorporates import OpenCorporatesClient, Company
    
    client = OpenCorporatesClient(api_key="your_key")  # key is optional
    for company in client.search_companies("Gazprom"):
        print(company.name, company.jurisdiction_code)
"""

import logging
from time import sleep
from typing import Generator

import httpx
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings

logger = logging.getLogger(__name__)


class Company(BaseModel):
    """
    Standardized company record.
    
    This is our canonical representation for company data regardless
    of which source it comes from (OpenCorporates, UK Companies House, etc.).
    
    Attributes:
        company_number: Official registration number within jurisdiction
        name: Current company name
        jurisdiction_code: ISO-style code (e.g., "us_de" for Delaware, "gb" for UK)
        incorporation_date: Date company was formed
        company_type: Legal form (e.g., "Limited Liability Company")
        current_status: Active, Dissolved, etc.
        registered_address: Official registered address
        officers: List of directors/officers with their details
    """
    
    company_number: str = Field(..., description="Official registration number")
    name: str = Field(..., description="Current legal name")
    jurisdiction_code: str = Field(..., description="Jurisdiction code (e.g., 'gb', 'us_de')")
    incorporation_date: str | None = Field(None, description="Date of incorporation")
    company_type: str | None = Field(None, description="Legal form/type")
    current_status: str | None = Field(None, description="Current status (Active, Dissolved, etc.)")
    registered_address: str | None = Field(None, description="Registered office address")
    officers: list[dict] = Field(default_factory=list, description="List of officers/directors")
    
    @property
    def unique_id(self) -> str:
        """Generate a unique identifier for this company."""
        return f"{self.jurisdiction_code}_{self.company_number}"
    
    def __str__(self) -> str:
        return f"{self.name} ({self.jurisdiction_code}/{self.company_number})"


class Officer(BaseModel):
    """
    Company officer (director, secretary, etc.) record.
    
    Attributes:
        name: Officer's name
        position: Role (e.g., "Director", "Secretary")
        start_date: When they took the position
        end_date: When they left (None if current)
        nationality: Nationality if known
        occupation: Occupation if known
    """
    
    name: str
    position: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    nationality: str | None = None
    occupation: str | None = None


class OpenCorporatesClient:
    """
    Client for the OpenCorporates API.
    
    OpenCorporates aggregates company data from registries worldwide.
    The API provides search and lookup capabilities.
    
    Note: For large-scale analysis, consider their bulk data products
    rather than API queries. The API is better suited for targeted lookups.
    
    Attributes:
        api_key: Optional API key for higher rate limits
        base_url: API base URL
        client: HTTP client instance
    
    Example:
        >>> client = OpenCorporatesClient()
        >>> companies = list(client.search_companies("Shell", jurisdiction_code="nl"))
        >>> print(f"Found {len(companies)} companies")
    """
    
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
    ):
        """
        Initialize the OpenCorporates client.
        
        Args:
            api_key: API key for authentication. Optional but recommended
                     for higher rate limits.
            base_url: API base URL. Defaults to settings.opencorporates_base_url.
        """
        self.api_key = api_key or settings.opencorporates_api_key
        self.base_url = base_url or settings.opencorporates_base_url
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(settings.http_timeout),
            follow_redirects=True,
        )
        
        # Track API usage for rate limiting
        self._request_count = 0
        
        logger.info(
            f"Initialized OpenCorporatesClient "
            f"(authenticated={self.api_key is not None})"
        )
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
    
    def _build_params(self, **kwargs) -> dict:
        """Build request parameters, adding API key if available."""
        params = {k: v for k, v in kwargs.items() if v is not None}
        if self.api_key:
            params["api_token"] = self.api_key
        return params
    
    def _rate_limit(self):
        """Apply rate limiting between requests."""
        self._request_count += 1
        sleep(settings.rate_limit_delay)
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def search_companies(
        self,
        query: str,
        jurisdiction_code: str | None = None,
        country_code: str | None = None,
        status: str | None = None,
        limit: int = 100,
    ) -> Generator[Company, None, None]:
        """
        Search for companies by name.
        
        Args:
            query: Search query (company name or partial name)
            jurisdiction_code: Limit to specific jurisdiction (e.g., "us_de", "gb")
            country_code: Limit to country (e.g., "us", "gb")
            status: Filter by status ("Active", "Dissolved", etc.)
            limit: Maximum number of results to return
        
        Yields:
            Company objects matching the search criteria.
        
        Example:
            >>> for company in client.search_companies("Acme", jurisdiction_code="us_de"):
            ...     print(company.name)
        """
        logger.debug(f"Searching companies: query={query}, jurisdiction={jurisdiction_code}")
        
        params = self._build_params(
            q=query,
            jurisdiction_code=jurisdiction_code,
            country_code=country_code,
            current_status=status,
            per_page=min(limit, 100),  # API max is 100 per page
        )
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/companies/search",
            params=params,
        )
        response.raise_for_status()
        
        data = response.json()
        companies = data.get("results", {}).get("companies", [])
        
        count = 0
        for result in companies:
            if count >= limit:
                break
            
            company_data = result.get("company", {})
            
            yield Company(
                company_number=company_data.get("company_number", ""),
                name=company_data.get("name", ""),
                jurisdiction_code=company_data.get("jurisdiction_code", ""),
                incorporation_date=company_data.get("incorporation_date"),
                company_type=company_data.get("company_type"),
                current_status=company_data.get("current_status"),
                registered_address=company_data.get("registered_address_in_full"),
            )
            count += 1
        
        logger.debug(f"Search returned {count} companies")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def get_company(
        self,
        jurisdiction_code: str,
        company_number: str,
    ) -> Company | None:
        """
        Get detailed information about a specific company.
        
        Args:
            jurisdiction_code: Jurisdiction code (e.g., "gb", "us_de")
            company_number: Official registration number
        
        Returns:
            Company object with full details, or None if not found.
        
        Example:
            >>> company = client.get_company("gb", "00445790")
            >>> if company:
            ...     print(company.name)
            ...     for officer in company.officers:
            ...         print(f"  - {officer['name']}: {officer['position']}")
        """
        logger.debug(f"Getting company: {jurisdiction_code}/{company_number}")
        
        params = self._build_params()
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/companies/{jurisdiction_code}/{company_number}",
            params=params,
        )
        
        if response.status_code == 404:
            logger.debug(f"Company not found: {jurisdiction_code}/{company_number}")
            return None
        
        response.raise_for_status()
        
        company_data = response.json().get("results", {}).get("company", {})
        
        # Extract officers
        officers = []
        for officer_wrapper in company_data.get("officers", []):
            officer = officer_wrapper.get("officer", {})
            officers.append({
                "name": officer.get("name"),
                "position": officer.get("position"),
                "start_date": officer.get("start_date"),
                "end_date": officer.get("end_date"),
                "nationality": officer.get("nationality"),
                "occupation": officer.get("occupation"),
            })
        
        return Company(
            company_number=company_data.get("company_number", ""),
            name=company_data.get("name", ""),
            jurisdiction_code=company_data.get("jurisdiction_code", ""),
            incorporation_date=company_data.get("incorporation_date"),
            company_type=company_data.get("company_type"),
            current_status=company_data.get("current_status"),
            registered_address=company_data.get("registered_address_in_full"),
            officers=officers,
        )
    
    def search_officers(
        self,
        query: str,
        jurisdiction_code: str | None = None,
        limit: int = 100,
    ) -> Generator[dict, None, None]:
        """
        Search for company officers by name.
        
        Useful for finding all directorships held by a person.
        
        Args:
            query: Officer name to search for
            jurisdiction_code: Limit to specific jurisdiction
            limit: Maximum number of results
        
        Yields:
            Dictionary with officer and company details.
        
        Example:
            >>> for result in client.search_officers("John Smith", jurisdiction_code="gb"):
            ...     print(f"{result['name']} at {result['company_name']}")
        """
        logger.debug(f"Searching officers: query={query}")
        
        params = self._build_params(
            q=query,
            jurisdiction_code=jurisdiction_code,
            per_page=min(limit, 100),
        )
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/officers/search",
            params=params,
        )
        response.raise_for_status()
        
        data = response.json()
        officers = data.get("results", {}).get("officers", [])
        
        count = 0
        for result in officers:
            if count >= limit:
                break
            
            officer = result.get("officer", {})
            company = officer.get("company", {})
            
            yield {
                "name": officer.get("name"),
                "position": officer.get("position"),
                "start_date": officer.get("start_date"),
                "end_date": officer.get("end_date"),
                "company_name": company.get("name"),
                "company_number": company.get("company_number"),
                "jurisdiction_code": company.get("jurisdiction_code"),
            }
            count += 1
    
    def get_jurisdiction_info(self, jurisdiction_code: str) -> dict | None:
        """
        Get information about a jurisdiction.
        
        Args:
            jurisdiction_code: Jurisdiction code (e.g., "gb", "us_de")
        
        Returns:
            Dictionary with jurisdiction details, or None if not found.
        """
        params = self._build_params()
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/jurisdictions/{jurisdiction_code}",
            params=params,
        )
        
        if response.status_code == 404:
            return None
        
        response.raise_for_status()
        
        return response.json().get("results", {}).get("jurisdiction", {})
    
    @property
    def request_count(self) -> int:
        """Number of API requests made by this client instance."""
        return self._request_count


# Jurisdiction code reference for common jurisdictions
JURISDICTION_CODES = {
    # United Kingdom
    "gb": "United Kingdom",
    
    # United States (by state)
    "us_de": "Delaware",
    "us_ny": "New York",
    "us_ca": "California",
    "us_nv": "Nevada",
    "us_wy": "Wyoming",
    
    # Offshore/Secrecy jurisdictions
    "vg": "British Virgin Islands",
    "ky": "Cayman Islands",
    "bm": "Bermuda",
    "pa": "Panama",
    "sc": "Seychelles",
    "mh": "Marshall Islands",
    "ws": "Samoa",
    "bz": "Belize",
    
    # Europe
    "nl": "Netherlands",
    "de": "Germany",
    "lu": "Luxembourg",
    "ie": "Ireland",
    "cy": "Cyprus",
    "mt": "Malta",
    "ch": "Switzerland",
    "li": "Liechtenstein",
    
    # Russia (limited access)
    "ru": "Russia",
    
    # Asia
    "hk": "Hong Kong",
    "sg": "Singapore",
    "ae": "United Arab Emirates",
}


if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    # Demo search
    with OpenCorporatesClient() as client:
        print("Searching for 'Shell' companies in Netherlands...")
        print("-" * 60)
        
        for i, company in enumerate(client.search_companies("Shell", jurisdiction_code="nl", limit=5)):
            print(f"{i+1}. {company.name}")
            print(f"   Number: {company.company_number}")
            print(f"   Status: {company.current_status}")
            print(f"   Type: {company.company_type}")
            print()
        
        print(f"\nTotal API requests: {client.request_count}")
