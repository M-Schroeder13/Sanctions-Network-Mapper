"""
UK Companies House API Client Module

Provides direct access to the UK Companies House API, which offers
comprehensive data on UK-registered companies including:
- Company profiles and filings
- Officer information
- Persons with Significant Control (PSC) - beneficial ownership

API Documentation: https://developer.company-information.service.gov.uk/

Registration: Free API key required - https://developer.company-information.service.gov.uk/

Rate Limits: 600 requests per 5 minutes

Usage:
    from src.ingest.uk_companies_house import UKCompaniesHouseClient
    
    client = UKCompaniesHouseClient(api_key="your_api_key")
    
    # Search for companies
    results = client.search_companies("Barclays")
    
    # Get beneficial owners (PSC data - very valuable!)
    psc_data = client.get_persons_significant_control("00026167")
"""

import logging
from time import sleep
from typing import Generator

import httpx
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import settings
from src.ingest.opencorporates import Company  # Reuse our standard Company model

logger = logging.getLogger(__name__)


class PersonWithSignificantControl(BaseModel):
    """
    Person with Significant Control (PSC) record.
    
    UK law requires companies to report anyone who:
    - Holds more than 25% of shares
    - Holds more than 25% of voting rights
    - Has the right to appoint/remove majority of directors
    - Has the right to exercise significant influence or control
    
    This is GOLD for beneficial ownership analysis.
    
    Attributes:
        name: Full name of the person
        nationality: Nationality
        country_of_residence: Where they live
        natures_of_control: List of control types
        notified_on: Date PSC was registered
        ceased_on: Date PSC status ended (if applicable)
        address: Service address (may be partial)
        date_of_birth: Month and year of birth
    """
    
    name: str | None = None
    nationality: str | None = None
    country_of_residence: str | None = None
    natures_of_control: list[str] = Field(default_factory=list)
    notified_on: str | None = None
    ceased_on: str | None = None
    address: dict | None = None
    date_of_birth: dict | None = None
    
    # For corporate PSCs (companies that control other companies)
    kind: str | None = None
    identification: dict | None = None
    
    @property
    def is_individual(self) -> bool:
        """Check if this PSC is an individual (vs a company)."""
        return self.kind == "individual-person-with-significant-control"
    
    @property
    def control_summary(self) -> str:
        """Get a readable summary of control type."""
        if not self.natures_of_control:
            return "Unknown"
        
        # Map API codes to readable descriptions
        control_map = {
            "ownership-of-shares-25-to-50-percent": "25-50% shares",
            "ownership-of-shares-50-to-75-percent": "50-75% shares",
            "ownership-of-shares-75-to-100-percent": "75-100% shares",
            "voting-rights-25-to-50-percent": "25-50% voting",
            "voting-rights-50-to-75-percent": "50-75% voting",
            "voting-rights-75-to-100-percent": "75-100% voting",
            "right-to-appoint-and-remove-directors": "appoints directors",
            "significant-influence-or-control": "significant influence",
        }
        
        controls = [control_map.get(c, c) for c in self.natures_of_control]
        return ", ".join(controls)


class UKCompaniesHouseClient:
    """
    Client for the UK Companies House API.
    
    Provides access to official UK company registry data including
    the critically important PSC (beneficial ownership) register.
    
    The UK was one of the first countries to require beneficial
    ownership disclosure, making this data uniquely comprehensive.
    
    Attributes:
        api_key: Required API key (free registration)
        base_url: API base URL
        client: HTTP client instance
    
    Example:
        >>> client = UKCompaniesHouseClient(api_key="your_key")
        >>> 
        >>> # Get beneficial owners - this is the key capability
        >>> psc_list = client.get_persons_significant_control("00026167")
        >>> for psc in psc_list:
        ...     print(f"{psc.name}: {psc.control_summary}")
    """
    
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
    ):
        """
        Initialize the UK Companies House client.
        
        Args:
            api_key: API key from Companies House developer portal.
                     Can also be set via UK_COMPANIES_HOUSE_API_KEY env var.
            base_url: API base URL. Defaults to settings value.
        
        Raises:
            ValueError: If no API key is provided.
        """
        self.api_key = api_key or settings.uk_companies_house_api_key
        self.base_url = base_url or settings.uk_companies_house_base_url
        
        if not self.api_key:
            logger.warning(
                "No UK Companies House API key provided. "
                "Get a free key at https://developer.company-information.service.gov.uk/"
            )
        
        # Companies House uses HTTP Basic Auth with API key as username
        auth = (self.api_key, "") if self.api_key else None
        
        self.client = httpx.Client(
            timeout=httpx.Timeout(settings.http_timeout),
            auth=auth,
            follow_redirects=True,
        )
        
        self._request_count = 0
        
        logger.info(
            f"Initialized UKCompaniesHouseClient "
            f"(authenticated={self.api_key is not None})"
        )
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
    
    def _rate_limit(self):
        """Apply rate limiting (600 req / 5 min = ~0.5 sec between requests)."""
        self._request_count += 1
        sleep(0.5)  # Conservative rate limiting
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def search_companies(
        self,
        query: str,
        limit: int = 100,
    ) -> list[dict]:
        """
        Search for UK companies by name.
        
        Args:
            query: Company name search query
            limit: Maximum results to return (max 100 per request)
        
        Returns:
            List of company search results with basic info.
        
        Example:
            >>> results = client.search_companies("Barclays Bank")
            >>> for r in results[:5]:
            ...     print(f"{r['title']} - {r['company_number']}")
        """
        if not self.api_key:
            logger.error("API key required for UK Companies House")
            return []
        
        logger.debug(f"Searching UK companies: {query}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/search/companies",
            params={
                "q": query,
                "items_per_page": min(limit, 100),
            },
        )
        response.raise_for_status()
        
        items = response.json().get("items", [])
        logger.debug(f"Found {len(items)} companies")
        
        return items
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def get_company(self, company_number: str) -> Company | None:
        """
        Get detailed company profile.
        
        Args:
            company_number: UK company registration number (e.g., "00026167")
        
        Returns:
            Company object with full details, or None if not found.
        
        Example:
            >>> company = client.get_company("00026167")
            >>> print(f"{company.name} - {company.current_status}")
        """
        if not self.api_key:
            logger.error("API key required")
            return None
        
        logger.debug(f"Getting UK company: {company_number}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/company/{company_number}"
        )
        
        if response.status_code == 404:
            logger.debug(f"Company not found: {company_number}")
            return None
        
        response.raise_for_status()
        
        data = response.json()
        
        # Build registered address string
        address_parts = []
        addr = data.get("registered_office_address", {})
        for field in ["address_line_1", "address_line_2", "locality", "region", "postal_code", "country"]:
            if addr.get(field):
                address_parts.append(addr[field])
        
        return Company(
            company_number=data.get("company_number", ""),
            name=data.get("company_name", ""),
            jurisdiction_code="gb",  # Always UK for this API
            incorporation_date=data.get("date_of_creation"),
            company_type=data.get("type"),
            current_status=data.get("company_status"),
            registered_address=", ".join(address_parts) if address_parts else None,
        )
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def get_officers(self, company_number: str) -> list[dict]:
        """
        Get current and past officers of a company.
        
        Args:
            company_number: UK company registration number
        
        Returns:
            List of officer records with appointment details.
        
        Example:
            >>> officers = client.get_officers("00026167")
            >>> for o in officers[:5]:
            ...     print(f"{o['name']} - {o['officer_role']}")
        """
        if not self.api_key:
            return []
        
        logger.debug(f"Getting officers for: {company_number}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/company/{company_number}/officers"
        )
        
        if response.status_code == 404:
            return []
        
        response.raise_for_status()
        
        return response.json().get("items", [])
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def get_persons_significant_control(
        self,
        company_number: str,
        include_ceased: bool = True,
    ) -> list[PersonWithSignificantControl]:
        """
        Get Persons with Significant Control (PSC) for a company.
        
        THIS IS THE KEY CAPABILITY for beneficial ownership analysis.
        
        PSC data reveals who actually controls a company, not just
        who the directors are. This includes:
        - Major shareholders (25%+)
        - Voting rights holders (25%+)
        - Those who can appoint/remove directors
        - Those with "significant influence"
        
        Args:
            company_number: UK company registration number
            include_ceased: Whether to include former PSCs
        
        Returns:
            List of PSC records with control details.
        
        Example:
            >>> psc_list = client.get_persons_significant_control("00026167")
            >>> for psc in psc_list:
            ...     if psc.is_individual:
            ...         print(f"{psc.name}: {psc.control_summary}")
            ...     else:
            ...         print(f"Corporate: {psc.identification}")
        """
        if not self.api_key:
            return []
        
        logger.debug(f"Getting PSC for: {company_number}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/company/{company_number}/persons-with-significant-control"
        )
        
        if response.status_code == 404:
            return []
        
        response.raise_for_status()
        
        items = response.json().get("items", [])
        
        psc_list = []
        for item in items:
            # Skip ceased PSCs if requested
            if not include_ceased and item.get("ceased_on"):
                continue
            
            psc = PersonWithSignificantControl(
                name=item.get("name"),
                nationality=item.get("nationality"),
                country_of_residence=item.get("country_of_residence"),
                natures_of_control=item.get("natures_of_control", []),
                notified_on=item.get("notified_on"),
                ceased_on=item.get("ceased_on"),
                address=item.get("address"),
                date_of_birth=item.get("date_of_birth"),
                kind=item.get("kind"),
                identification=item.get("identification"),
            )
            psc_list.append(psc)
        
        logger.debug(f"Found {len(psc_list)} PSCs for {company_number}")
        
        return psc_list
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
    )
    def get_filing_history(
        self,
        company_number: str,
        limit: int = 25,
    ) -> list[dict]:
        """
        Get recent filing history for a company.
        
        Useful for tracking changes and identifying suspicious patterns.
        
        Args:
            company_number: UK company registration number
            limit: Maximum number of filings to return
        
        Returns:
            List of filing records with dates and descriptions.
        """
        if not self.api_key:
            return []
        
        logger.debug(f"Getting filing history for: {company_number}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/company/{company_number}/filing-history",
            params={"items_per_page": min(limit, 100)},
        )
        
        if response.status_code == 404:
            return []
        
        response.raise_for_status()
        
        return response.json().get("items", [])
    
    def search_officers(
        self,
        query: str,
        limit: int = 100,
    ) -> Generator[dict, None, None]:
        """
        Search for officers across all UK companies.
        
        Useful for finding all directorships held by a person.
        
        Args:
            query: Officer name to search
            limit: Maximum results to return
        
        Yields:
            Officer records with company associations.
        """
        if not self.api_key:
            return
        
        logger.debug(f"Searching UK officers: {query}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/search/officers",
            params={
                "q": query,
                "items_per_page": min(limit, 100),
            },
        )
        response.raise_for_status()
        
        for item in response.json().get("items", []):
            yield item
    
    def search_disqualified_officers(
        self,
        query: str,
        limit: int = 100,
    ) -> Generator[dict, None, None]:
        """
        Search the register of disqualified directors.
        
        Disqualified directors are banned from running companies,
        often due to fraud, misconduct, or unfitness.
        
        Args:
            query: Name to search
            limit: Maximum results
        
        Yields:
            Records of disqualified directors.
        """
        if not self.api_key:
            return
        
        logger.debug(f"Searching disqualified officers: {query}")
        
        self._rate_limit()
        
        response = self.client.get(
            f"{self.base_url}/search/disqualified-officers",
            params={
                "q": query,
                "items_per_page": min(limit, 100),
            },
        )
        response.raise_for_status()
        
        for item in response.json().get("items", []):
            yield item
    
    @property
    def request_count(self) -> int:
        """Number of API requests made by this client."""
        return self._request_count


if __name__ == "__main__":
    import sys
    import os
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    
    api_key = os.environ.get("UK_COMPANIES_HOUSE_API_KEY")
    
    if not api_key:
        print("Set UK_COMPANIES_HOUSE_API_KEY environment variable to run demo")
        print("Get a free key at: https://developer.company-information.service.gov.uk/")
        sys.exit(1)
    
    with UKCompaniesHouseClient(api_key=api_key) as client:
        # Demo: Get PSC data for a well-known company
        company_number = "00026167"  # Barclays Bank
        
        print(f"Getting company info for {company_number}...")
        company = client.get_company(company_number)
        if company:
            print(f"Company: {company.name}")
            print(f"Status: {company.current_status}")
            print(f"Type: {company.company_type}")
            print()
        
        print("Getting Persons with Significant Control...")
        print("-" * 60)
        
        psc_list = client.get_persons_significant_control(company_number)
        for psc in psc_list:
            if psc.is_individual:
                print(f"Individual: {psc.name}")
                print(f"  Nationality: {psc.nationality}")
                print(f"  Control: {psc.control_summary}")
            else:
                print(f"Corporate PSC: {psc.kind}")
                if psc.identification:
                    print(f"  ID: {psc.identification}")
            print()
        
        print(f"\nTotal API requests: {client.request_count}")
