// writes the EcalMapping object to a POOL-ORA database
// does not associate IOV information!
// remember to set POOL_AUTH_USER and POOL_AUTH_PASSWORD when using oracle

#include "CondFormats/EcalObjects/interface/EcalMapping.h"
#include "CondCore/DBCommon/interface/DBWriter.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"
#include <iostream>

using namespace std;

int main(int argc, char* argv[]) {

  if (argc != 2) {
    cerr << "usage:  " << argv[0] << " connString" << endl;
    return 1;
  }

  string connString = argv[1];
  string containerName = "EcalMapping";

  cout << "Building EcalMapping object..." << flush;
  EcalMapping ecm;
  ecm.buildMapping();
  cout << "Done." << endl;

  cout << "Connecting to database " << connString << "..." << flush;

  cond::DBWriter db(connString);
  cout << "Done." << endl;
  
  db.startTransaction();
 
  cout << "Writing mapping object..." << endl;
  db.write(&ecm, containerName);
  cout << "Done." << endl;
  
  db.commitTransaction();
  
  cout << endl << "All Done." << endl;
  return 0;
}
