from pathlib import Path

import pytest

from fprime_cosmos.dictionary import FprimeDictionary

DATA = Path(__file__).parent / "data"
REFERENCE_DICTIONARY = DATA / "ReferenceDeploymentTopologyDictionary.json"


@pytest.fixture(scope="session")
def reference_dictionary() -> FprimeDictionary:
    return FprimeDictionary(REFERENCE_DICTIONARY)


CFS_DICTIONARY = DATA / "FPrimeDeploymentTopologyDictionary.json"
CFS_PLUGIN = DATA / "cfs_plugin"


@pytest.fixture(scope="session")
def cfs_dictionary() -> FprimeDictionary:
    return FprimeDictionary(CFS_DICTIONARY)
