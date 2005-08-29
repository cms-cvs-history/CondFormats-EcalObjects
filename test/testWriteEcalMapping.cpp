#include "CondFormats/EcalObjects/interface/EcalMapping.h"
#include "CondCore/DBCommon/interface/DBWriter.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"
#include <iostream>

using namespace std;

int main(int argc, char* argv[]) {
  string connString = argv[1];  
  string containerName = argv[2];

  cout << "Building map object..." << flush;
  EcalMapping ecm;
  ecm.buildMapping();
  cout << "Done." << endl;

  cout << "Connecting to database " << connString << "..." << flush;

  cond::DBWriter db(connString);
  cout << "Done." << endl;

  if (!db.containerExists(containerName)) {
    
    db.startTransaction();

 
    cout << "Creating Container " << containerName << "..." << flush;
    db.createContainer(containerName);
    cout << "Done." << endl;
    
    cout << "Writing mapping object..." << endl;
    db.write(&ecm);
    cout << "Done." << endl;
    
    db.commitTransaction();
  } else {
    cerr << "Container exists... clean out database" << endl;
  }    

  cout << endl << "All Done." << endl;
  return 0;
}
