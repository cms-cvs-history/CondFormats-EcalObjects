#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondCore/IOVService/interface/IOV.h"

#include "FWCore/Framework/interface/IOVSyncValue.h"

#include "CondCore/MetaDataService/interface/MetaData.h"

#include <string>
#include <map>
#include <iostream>
#include <fstream>
#include <cstdio>
#include <typeinfo>

#include "CondFormats/EcalObjects/interface/EcalWeightXtalGroups.h"
#include "CondFormats/EcalObjects/interface/EcalXtalGroupId.h"
#include "CondFormats/EcalObjects/interface/EcalTBWeights.h"
#include "CondFormats/EcalObjects/interface/EcalWeightSet.h"
#include "CondFormats/EcalObjects/interface/EcalWeight.h"
#include "CondFormats/EcalObjects/interface/EcalIntercalibConstants.h"
#include "CondFormats/EcalObjects/interface/EcalGainRatios.h"
#include "CondFormats/EcalObjects/interface/EcalMGPAGainRatio.h"
#include "CondFormats/EcalObjects/interface/EcalADCToGeVConstant.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"

using namespace std;

//-------------------------------------------------------------
class Application {
  //-------------------------------------------------------------
private:
  std::map<std::string, std::string> objNameMap;
  std::string connection_;
  std::string tag_;

public:
  void* generate(std::string objectName);
  EcalWeightXtalGroups* generateEcalWeightXtalGroups();
  EcalTBWeights* generateEcalTBWeights();
  EcalADCToGeVConstant* generateEcalADCToGeVConstant();
  EcalIntercalibConstants* generateEcalIntercalibConstants();
  EcalGainRatios* generateEcalGainRatios();
  EcalPedestals* generateEcalPedestals();
  template <class T> void writeObjectsIntoDB(unsigned long firstrun, unsigned long lastrun, unsigned int interval);

  Application(const std::string& connection,  const std::string& tag);
  ~Application();

};
//-------------------------------------------------------------
//-------------------------------------------------------------

//-------------------------------------------------------------
Application::Application(const std::string& connection, const std::string& tag) {
//-------------------------------------------------------------

  connection_ = connection;
  tag_ = tag;

  std::string objectTypeName;
  std::string objectName;
  
  objectName = "EcalWeightXtalGroups";
  objectTypeName = std::string( typeid(EcalWeightXtalGroups).name() );
  objNameMap[objectTypeName] = objectName;

  objectName = "EcalTBWeights";
  objectTypeName = std::string( typeid(EcalTBWeights).name() );
  objNameMap[objectTypeName] = objectName;

  objectName = "EcalADCToGeVConstant";
  objectTypeName = std::string( typeid(EcalADCToGeVConstant).name() );
  objNameMap[objectTypeName] = objectName;

  objectName = "EcalGainRatios";
  objectTypeName = std::string( typeid(EcalGainRatios).name() );
  objNameMap[objectTypeName] = objectName;

  objectName = "EcalIntercalibConstants";
  objectTypeName = std::string( typeid(EcalIntercalibConstants).name() );
  objNameMap[objectTypeName] = objectName;

  objectName = "EcalPedestals";
  objectTypeName = std::string( typeid(EcalPedestals).name() );
  objNameMap[objectTypeName] = objectName;

  cout << "Done." << endl;

}

//-------------------------------------------------------------
Application::~Application() {
//-------------------------------------------------------------
}


void*
Application::generate(std::string objectName) {

  if (objectName == "EcalWeightXtalGroups") {
    return generateEcalWeightXtalGroups();
  } else if (objectName == "EcalTBWeights") {
    return generateEcalTBWeights();
  } else if (objectName == "EcalADCToGeVConstant") {
    return generateEcalADCToGeVConstant();
  } else if (objectName == "EcalGainRatios") {
    return generateEcalGainRatios();
  } else if (objectName == "EcalIntercalibConstants") {
    return generateEcalIntercalibConstants();
  } else if (objectName == "EcalPedestals") {
    return generateEcalPedestals();
  } else {
    return 0;
  }

}

//-------------------------------------------------------------
EcalWeightXtalGroups*
Application::generateEcalWeightXtalGroups() {
//-------------------------------------------------------------

  EcalWeightXtalGroups* xtalGroups = new EcalWeightXtalGroups();
  for(int ieta=-EBDetId::MAX_IETA; ieta<=EBDetId::MAX_IETA; ++ieta) {
    if(ieta==0) continue;
    for(int iphi=EBDetId::MIN_IPHI; iphi<=EBDetId:: MAX_IPHI; ++iphi) {
      EBDetId ebid(ieta,iphi);
      xtalGroups->setValue(ebid.rawId(), EcalXtalGroupId(ieta) ); // define rings in eta
    }
  }
  return xtalGroups;
}

//-------------------------------------------------------------
EcalTBWeights*
Application::generateEcalTBWeights() {
//-------------------------------------------------------------

  EcalTBWeights* tbwgt = new EcalTBWeights();

  // create weights for each distinct group ID
  int nMaxTDC = 10;
  for(int igrp=-EBDetId::MAX_IETA; igrp<=EBDetId::MAX_IETA; ++igrp) {
    if(igrp==0) continue;
    for(int itdc=1; itdc<=nMaxTDC; ++itdc) {
      // generate random number
      double r = (double)std::rand()/( double(RAND_MAX)+double(1) );

      // make a new set of weights
      EcalWeightSet wgt;
      EcalWeightSet::EcalWeightMatrix& mat1 = wgt.getWeightsBeforeGainSwitch();
      EcalWeightSet::EcalWeightMatrix& mat2 = wgt.getWeightsAfterGainSwitch();

      for(size_t i=0; i<3; ++i) {
      std::vector<EcalWeight> tv1, tv2;
        for(size_t j=0; j<10; ++j) {
          double ww = igrp*itdc*r + i*10. + j;
          //std::cout << "row: " << i << " col: " << j << " -  val: " << ww  << std::endl;
          tv1.push_back( EcalWeight(ww) );
          tv2.push_back( EcalWeight(100+ww) );
        }
        mat1.push_back(tv1);
        mat2.push_back(tv2);
      }

      // fill the chi2 matrcies
      r = (double)std::rand()/( double(RAND_MAX)+double(1) );
      EcalWeightSet::EcalWeightMatrix& mat3 = wgt.getChi2WeightsBeforeGainSwitch();
      EcalWeightSet::EcalWeightMatrix& mat4 = wgt.getChi2WeightsAfterGainSwitch();
      for(size_t i=0; i<10; ++i) {
        std::vector<EcalWeight> tv1, tv2;
        for(size_t j=0; j<10; ++j) {
          double ww = igrp*itdc*r + i*10. + j;
          tv1.push_back( EcalWeight(1000+ww) );
          tv2.push_back( EcalWeight(1000+100+ww) );
        }
        mat3.push_back(tv1);
        mat4.push_back(tv2);
      }

      // put the weight in the container
      tbwgt->setValue(std::make_pair(igrp,itdc), wgt);
    }
  }
  return tbwgt;
}


//-------------------------------------------------------------
EcalADCToGeVConstant*
Application::generateEcalADCToGeVConstant() {
//-------------------------------------------------------------
  
  double r = (double)std::rand()/( double(RAND_MAX)+double(1) );
  EcalADCToGeVConstant* agc = new EcalADCToGeVConstant(36.+r*4., 60.+r*4);
  return agc;
}


//-------------------------------------------------------------
EcalIntercalibConstants*
Application::generateEcalIntercalibConstants() {
//-------------------------------------------------------------

  EcalIntercalibConstants* ical = new EcalIntercalibConstants();

  for(int ieta=-EBDetId::MAX_IETA; ieta<=EBDetId::MAX_IETA; ++ieta) {
    if(ieta==0) continue;
    for(int iphi=EBDetId::MIN_IPHI; iphi<=EBDetId:: MAX_IPHI; ++iphi) {

      EBDetId ebid(ieta,iphi);

      double r = (double)std::rand()/( double(RAND_MAX)+double(1) );
      ical->setValue( ebid.rawId(), 0.85 + r*0.3 );
    } // loop over phi
  } // loop over eta
  return ical;
}


//-------------------------------------------------------------
EcalGainRatios*
Application::generateEcalGainRatios() {
//-------------------------------------------------------------

  // create gain ratios
  EcalGainRatios* gratio = new EcalGainRatios;

  for(int ieta=-EBDetId::MAX_IETA; ieta<=EBDetId::MAX_IETA; ++ieta) {
    if(ieta==0) continue;
    for(int iphi=EBDetId::MIN_IPHI; iphi<=EBDetId:: MAX_IPHI; ++iphi) {

      EBDetId ebid(ieta,iphi);

      double r = (double)std::rand()/( double(RAND_MAX)+double(1) );

      EcalMGPAGainRatio gr;
      gr.setGain12Over6( 1.9 + r*0.2 );
      gr.setGain6Over1( 5.9 + r*0.2 );

      gratio->setValue( ebid.rawId(), gr );

    } // loop over phi
  } // loop over eta
  return gratio;
}

//-------------------------------------------------------------
EcalPedestals*
Application::generateEcalPedestals() {
//-------------------------------------------------------------

  EcalPedestals* peds = new EcalPedestals();
  EcalPedestals::Item item;
  for(int iEta=-EBDetId::MAX_IETA; iEta<=EBDetId::MAX_IETA ;++iEta) {
    if(iEta==0) continue;
    for(int iPhi=EBDetId::MIN_IPHI; iPhi<=EBDetId::MAX_IPHI; ++iPhi) {
      item.mean_x1  = 200.*( (double)std::rand()/(double(RAND_MAX)+double(1)) );
      item.rms_x1   = (double)std::rand()/(double(RAND_MAX)+double(1));
      item.mean_x6  = 1200.*( (double)std::rand()/(double(RAND_MAX)+double(1)) );
      item.rms_x6   = 6.*( (double)std::rand()/(double(RAND_MAX)+double(1)) );
      item.mean_x12 = 2400.*( (double)std::rand()/(double(RAND_MAX)+double(1)) );
      item.rms_x12  = 12.*( (double)std::rand()/(double(RAND_MAX)+double(1)) );

      EBDetId ebdetid(iEta,iPhi);
      peds->m_pedestals.insert(std::make_pair(ebdetid.rawId(),item));
    }
  }
  return peds;
}

//-------------------------------------------------------------
template <class T>
void Application::writeObjectsIntoDB(unsigned long firstrun, unsigned long lastrun, unsigned int interval) {
//-------------------------------------------------------------

  // Set up DB Connection
  cond::ServiceLoader* loader;
  cond::DBSession*     session;
  cond::MetaData*      metadata_svc;

  cout << "Setting up DB Connection..." << flush;
  loader = new cond::ServiceLoader;
  loader->loadMessageService(cond::Error);

  session = new cond::DBSession(connection_);
  session->connect(cond::ReadWriteCreate);

  metadata_svc = new cond::MetaData(connection_, *loader);
  metadata_svc->connect();

  cout << "Done." << endl;


  //define iov objects
  cond::DBWriter* iovWriter  = new cond::DBWriter(*session, "cond::IOV");
  cond::IOV* iov = new cond::IOV;
  std::string token;

  // Get object's typeid string name
  std::string objectTypeName = typeid(T).name();

  // Get the object's normal name and define a tag
  std::string objectName = objNameMap[objectTypeName];
  std::string tag = objectName + "_" + tag_;
  cout << "Using tag " << tag << endl;

  // Create a DBWriter for this object type
  cout << "Creating writer for " << objectName << endl;
  cond::DBWriter* objectWriter = new cond::DBWriter(*session, objectName);

  // Loop through each of the runs
  for(unsigned long irun=firstrun; irun<=lastrun; ++irun) {

    // Skip if irun is not a factor of the interval
    if (irun % interval != 0) {
      continue;
    }

    // Arguments 0 0 mean infine IOV
    if (firstrun == 0 && lastrun == 0) {
      cout << "Infinite IOV mode" << endl;
      irun = edm::IOVSyncValue::endOfTime().eventID().run();
    }

    cout << "Starting Transaction for run " << irun << "..." << flush;
    session->startUpdateTransaction();

    // Generate the object
    cout << "Generating " << objectName << "..." << flush;
    T* condObject;
    void* generatedObject = generate(objectName);
    if (generatedObject != 0) {
      condObject = reinterpret_cast<T*>( generatedObject );
    } else {
      cerr << "ERROR:  NULL returned from generate()" << endl;
      exit(-1);
    }

    // Write the object
    cout << "Writing..." << flush;
    token = objectWriter->markWrite(condObject);    

    // Insert the IOV
    iov->iov.insert(std::make_pair(irun, token));
  
    cout << "Committing Session..." << flush;
    session->commit();
    cout << "Done." << endl;

    // End loop on infinite IOV
    if (firstrun == 0 && lastrun == 0) {
      break;
    }
  }

  // writing iov's and adding mapping for meta data

  // new session to write IOV objects
  cout << "Starting transaction for IOV objects..." << flush;
  session->startUpdateTransaction();

  cout << "Writing..." << flush;
  std::string iovToken = iovWriter->markWrite(iov);

  cout << "Commit..." << flush;
  session->commit();
  cout << "Done." << endl;

  // finally add mapping for meta data
  cout << "Adding metadata to objects..." << flush;
  metadata_svc->addMapping(tag, iovToken);
  cout << "Done." << endl;

  // Clean up
  cout << "Cleaning up..." << flush;
  delete objectWriter;
  delete loader;
  session->disconnect();
  delete session;
  metadata_svc->disconnect();
  delete metadata_svc;
  delete iovWriter;
  cout << "Done." << endl;

} // writeObjectsIntoDB()




// =========
int main(int argc, char* argv[]) {
// =========

  // usage
  if (argc < 6) {
    cout << argv[0]
         << " <connection> <first run> <last run> <interval> <tag> <object1> <object2> ..." << endl
	 << " Where objectX is one of:  " << endl
	 << " EcalWeightXtalGroups, EcalTBWeights, EcalADCToGeVConstant, EcalIntercalibConstants, EcalGainRatios" << endl
	 << " Special conditions:" << endl
	 << "   - Specify <first run>, <last run> and <interval> of 0 to write one object of infinite IOV" << endl
	 << "   - Write no <objectX> list in order to write ALL object types" << endl
         << endl;
    exit(-1);
  }

  // parse the command line arguments
  std::string connection = argv[1];
  unsigned long firstRun = (unsigned long)atoi(argv[2]);
  unsigned long lastRun = (unsigned long)atoi(argv[3]);
  unsigned int interval = (unsigned int)atoi(argv[4]);
  std::string tag = argv[5];

  vector<std::string> objectList;
  for (int i=6; i<argc; i++) {
    objectList.push_back(std::string(argv[i]));
  }

  Application app(connection, tag);

  try {
    std::vector<std::string>::const_iterator ci;
    for (ci = objectList.begin(); ci != objectList.end(); ci++) {

      if (*ci == "EcalWeightXtalGroups") {
	app.writeObjectsIntoDB<EcalWeightXtalGroups>(firstRun, lastRun, interval);
      } else if (*ci =="EcalTBWeights") {
	app.writeObjectsIntoDB<EcalTBWeights>(firstRun, lastRun, interval);
      } else if (*ci == "EcalADCToGeVConstant") {
	app.writeObjectsIntoDB<EcalADCToGeVConstant>(firstRun, lastRun, interval);
      } else if (*ci ==  "EcalIntercalibConstants") {
	app.writeObjectsIntoDB<EcalIntercalibConstants>(firstRun, lastRun, interval);
      } else if (*ci ==  "EcalGainRatios") {
	app.writeObjectsIntoDB<EcalGainRatios>(firstRun, lastRun, interval);
      } else if (*ci ==  "EcalPedestals") {
	app.writeObjectsIntoDB<EcalPedestals>(firstRun, lastRun, interval);
      } else {
	cout << "ERROR:  Object " << *ci << " is not supported by this program." << endl;
      }

    }
  } catch(cond::Exception &e) {
    cout << e.what() << endl;
  } catch(std::exception &e) {
    cout << e.what() << endl;
  } catch(...) {
    cout << "Unknown exception" << endl;
  }

  return 0;

}
