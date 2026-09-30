from pathlib import Path

import pytest

from fprime_cosmos.dictionary import FprimeDictionary

DATA = Path(__file__).parent / "data"
REFERENCE_DICTIONARY = DATA / "ReferenceDeploymentTopologyDictionary.json"


@pytest.fixture(scope="session")
def reference_dictionary() -> FprimeDictionary:
    return FprimeDictionary(REFERENCE_DICTIONARY)
