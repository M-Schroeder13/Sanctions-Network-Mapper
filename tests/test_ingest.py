"""
Tests for data ingestion modules.

These tests verify the parsing and data extraction logic.
Network tests are mocked to avoid external API calls.
"""

import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

from src.ingest.opensanctions import OpenSanctionsClient
from src.ingest.opencorporates import Company, OpenCorporatesClient
from src.ingest.uk_companies_house import PersonWithSignificantControl


# =============================================================================
# OpenSanctions Tests
# =============================================================================

class TestOpenSanctionsClient:
    """Tests for OpenSanctionsClient parsing logic."""
    
    @pytest.fixture
    def sample_entities_file(self, tmp_path: Path) -> Path:
        """Create a sample NDJSON file with test entities."""
        entities = [
            {
                "id": "ofac-12345",
                "schema": "Person",
                "caption": "Test Person",
                "datasets": ["us_ofac_sdn", "eu_fsf"],
                "first_seen": "2022-01-01",
                "last_seen": "2024-01-01",
                "properties": {
                    "name": ["Test Person", "Person Test"],
                    "alias": ["TP", "Testy"],
                    "country": ["RU"],
                    "birthDate": ["1970-01-01"],
                    "nationality": ["Russian"],
                    "innCode": ["1234567890"],
                }
            },
            {
                "id": "ofac-67890",
                "schema": "Company",
                "caption": "Test Company LLC",
                "datasets": ["us_ofac_sdn"],
                "properties": {
                    "name": ["Test Company LLC"],
                    "country": ["RU", "CY"],
                    "jurisdiction": ["cy"],
                    "registrationNumber": ["HE123456"],
                    "incorporationDate": ["2015-06-15"],
                    "ogrnCode": ["1234567890123"],
                }
            },
            {
                "id": "ofac-11111",
                "schema": "LegalEntity",
                "caption": "Shell Corp",
                "datasets": ["us_ofac_sdn"],
                "properties": {
                    "name": ["Shell Corp"],
                    "ownershipOwner": ["ofac-12345"],  # Owned by Test Person
                }
            },
        ]
        
        filepath = tmp_path / "test_entities.json"
        with open(filepath, "w") as f:
            for entity in entities:
                f.write(json.dumps(entity) + "\n")
        
        return filepath
    
    def test_parse_entities_basic(self, sample_entities_file: Path, tmp_path: Path):
        """Test basic entity parsing."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.parse_entities(sample_entities_file)
        
        assert len(df) == 3
        assert "entity_id" in df.columns
        assert "schema" in df.columns
        assert "caption" in df.columns
    
    def test_parse_entities_schemas(self, sample_entities_file: Path, tmp_path: Path):
        """Test that schema types are correctly parsed."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.parse_entities(sample_entities_file)
        
        schemas = df["schema"].to_list()
        assert "Person" in schemas
        assert "Company" in schemas
        assert "LegalEntity" in schemas
    
    def test_parse_entities_person_fields(self, sample_entities_file: Path, tmp_path: Path):
        """Test person-specific field extraction."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.parse_entities(sample_entities_file)
        
        person = df.filter(pl.col("schema") == "Person").row(0, named=True)
        
        assert person["entity_id"] == "ofac-12345"
        assert "Test Person" in person["names"]
        assert person["birth_date"] == "1970-01-01"
        assert person["inn_code"] == "1234567890"
    
    def test_parse_entities_company_fields(self, sample_entities_file: Path, tmp_path: Path):
        """Test company-specific field extraction."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.parse_entities(sample_entities_file)
        
        company = df.filter(pl.col("schema") == "Company").row(0, named=True)
        
        assert company["entity_id"] == "ofac-67890"
        assert company["jurisdiction"] == "cy"
        assert company["registration_number"] == "HE123456"
        assert company["ogrn_code"] == "1234567890123"
    
    def test_parse_entities_multi_valued_fields(self, sample_entities_file: Path, tmp_path: Path):
        """Test that multi-valued fields are pipe-delimited."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.parse_entities(sample_entities_file)
        
        person = df.filter(pl.col("schema") == "Person").row(0, named=True)
        
        # Names should be pipe-separated
        assert "|" in person["names"]
        names = person["names"].split("|")
        assert "Test Person" in names
        assert "Person Test" in names
        
        # Datasets should be comma-separated
        assert "," in person["datasets"]
        datasets = person["datasets"].split(",")
        assert "us_ofac_sdn" in datasets
    
    def test_extract_relationships(self, sample_entities_file: Path, tmp_path: Path):
        """Test relationship extraction."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        df = client.extract_relationships(sample_entities_file)
        
        assert len(df) >= 1
        
        # Check ownership relationship
        ownership = df.filter(pl.col("relationship_type") == "owned_by").row(0, named=True)
        
        assert ownership["source_id"] == "ofac-11111"
        assert ownership["target_id"] == "ofac-12345"
    
    def test_get_first_helper(self, tmp_path: Path):
        """Test the _get_first helper method."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        # With values
        assert client._get_first({"key": ["a", "b"]}, "key") == "a"
        
        # Empty list
        assert client._get_first({"key": []}, "key") is None
        
        # Missing key
        assert client._get_first({}, "key") is None
    
    def test_dataset_validation(self, tmp_path: Path):
        """Test that invalid datasets raise ValueError."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        with pytest.raises(ValueError, match="Unknown dataset"):
            client.download_dataset("invalid_dataset")


# =============================================================================
# Company Model Tests
# =============================================================================

class TestCompanyModel:
    """Tests for the Company Pydantic model."""
    
    def test_company_creation(self):
        """Test basic company model creation."""
        company = Company(
            company_number="12345678",
            name="Test Company Ltd",
            jurisdiction_code="gb",
            incorporation_date="2020-01-15",
            company_type="ltd",
            current_status="active",
            registered_address="123 Test Street, London",
        )
        
        assert company.company_number == "12345678"
        assert company.name == "Test Company Ltd"
        assert company.jurisdiction_code == "gb"
    
    def test_company_unique_id(self):
        """Test unique_id property."""
        company = Company(
            company_number="12345678",
            name="Test Company",
            jurisdiction_code="gb",
        )
        
        assert company.unique_id == "gb_12345678"
    
    def test_company_str(self):
        """Test string representation."""
        company = Company(
            company_number="12345678",
            name="Test Company",
            jurisdiction_code="gb",
        )
        
        assert str(company) == "Test Company (gb/12345678)"
    
    def test_company_optional_fields(self):
        """Test that optional fields default to None."""
        company = Company(
            company_number="12345678",
            name="Test Company",
            jurisdiction_code="gb",
        )
        
        assert company.incorporation_date is None
        assert company.company_type is None
        assert company.current_status is None
        assert company.registered_address is None
        assert company.officers == []


# =============================================================================
# PSC Model Tests
# =============================================================================

class TestPersonWithSignificantControl:
    """Tests for the PSC model."""
    
    def test_individual_psc(self):
        """Test individual PSC creation."""
        psc = PersonWithSignificantControl(
            name="John Smith",
            nationality="British",
            country_of_residence="England",
            natures_of_control=["ownership-of-shares-25-to-50-percent"],
            notified_on="2020-01-01",
            kind="individual-person-with-significant-control",
        )
        
        assert psc.name == "John Smith"
        assert psc.is_individual is True
    
    def test_corporate_psc(self):
        """Test corporate PSC."""
        psc = PersonWithSignificantControl(
            kind="corporate-entity-person-with-significant-control",
            identification={
                "legal_authority": "Companies Act 2006",
                "legal_form": "limited company",
            },
            natures_of_control=["ownership-of-shares-75-to-100-percent"],
        )
        
        assert psc.is_individual is False
    
    def test_control_summary(self):
        """Test control summary generation."""
        psc = PersonWithSignificantControl(
            name="Test Person",
            natures_of_control=[
                "ownership-of-shares-25-to-50-percent",
                "voting-rights-25-to-50-percent",
            ],
        )
        
        summary = psc.control_summary
        assert "25-50% shares" in summary
        assert "25-50% voting" in summary
    
    def test_control_summary_empty(self):
        """Test control summary with no controls."""
        psc = PersonWithSignificantControl(name="Test")
        
        assert psc.control_summary == "Unknown"


# =============================================================================
# Integration Tests (require network - skip in CI)
# =============================================================================

@pytest.mark.skip(reason="Requires network access - run manually")
class TestOpenSanctionsIntegration:
    """Integration tests that require network access."""
    
    def test_download_and_parse(self, tmp_path: Path):
        """Test actual download and parse."""
        client = OpenSanctionsClient(cache_dir=tmp_path)
        
        # Download smallest dataset
        filepath = client.download_dataset("sanctions")
        
        assert filepath.exists()
        assert filepath.stat().st_size > 0
        
        # Parse a few entities
        df = client.parse_entities(filepath)
        
        assert len(df) > 0
        assert "entity_id" in df.columns
