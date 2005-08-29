#include "CondFormats/EcalObjects/interface/EcalMapping.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"
#include <iostream>

using namespace std;

int main(int argc, char* argv[]) {

  cout << "Building mapping..." << flush;
  EcalMapping ecm;
  ecm.buildMapping();
  cout << "Done." << endl;

  EcalMapping::crystalAnglesPair angles;
  int logicId;
  cms::EBDetId detid;

  for (int SM=1; SM <= 36; ++SM) {
    for (int xtal=1; xtal <= 1700; ++xtal) {
      angles = ecm.crystalNumberToAngles(SM, xtal);
      logicId = ecm.crystalNumberToLogicID(SM, xtal);
      cout << "SM " << SM <<
	" xtal " << xtal <<
	" logic_id " << logicId <<
	" <-> ieta " << angles.ieta <<
	" iphi " << angles.iphi << endl;

      detid = ecm.crystalNumberToEBDetID(SM, xtal);
      cout << "EBDetId raw ID " << detid.rawId() << endl;

      cout << "Mapping Lookup raw ID " << ecm.lookup(logicId).rawId() << endl;
      cout << endl;

    }
  }

  return 0;
}
