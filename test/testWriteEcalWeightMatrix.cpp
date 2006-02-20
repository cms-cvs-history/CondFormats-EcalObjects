#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/DBCommon/interface/DBSession.h"
#include "CondCore/DBCommon/interface/Exception.h"
#include "CondCore/DBCommon/interface/ServiceLoader.h"
#include "CondCore/DBCommon/interface/ConnectMode.h"
#include "CondCore/DBCommon/interface/MessageLevel.h"
#include "CondFormats/Calibration/interface/Pedestals.h"
#include "CondCore/IOVService/interface/IOV.h"


#include "FWCore/Framework/interface/IOVSyncValue.h"

#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/EcalObjects/interface/EcalWeightRecAlgoWeights.h"
#include "CondFormats/EcalObjects/interface/EcalWeight.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"


#include "CondCore/MetaDataService/interface/MetaData.h"
#include <string>
#include <map>
#include <iostream>

using namespace std;

int main(){
  cout << "Setting up DB Connection..." << flush;
  cond::ServiceLoader* loader = new cond::ServiceLoader;
  loader->loadMessageService(cond::Error);
  //  loader->loadAuthenticationService(cond::XML);  // unneccessary?

  string contact("sqlite_file:ecalcalib.db");
  cond::DBSession* session = new cond::DBSession(contact);
  cond::MetaData* metadata_svc = new cond::MetaData(contact, *loader);
  cout << "Done." << endl;

  try {
    cout << "Creating Writers..." << flush;
    session->connect(cond::ReadWriteCreate);
    cond::DBWriter wgtWriter(*session, "EcalWeightRecAlgoWeights");
    cond::DBWriter pedWriter(*session, "EcalPedestals");
    cond::DBWriter iovWriter(*session, "IOV");
    cout << "Done." << endl;

    cout << "Starting Transaction." << endl;
    session->startUpdateTransaction();

    cond::IOV* wgt_iov = new cond::IOV;

    cout << "Building EcalWeightRecAlgoWeights." << endl;
    EcalWeightRecAlgoWeights* wgt = new EcalWeightRecAlgoWeights;

    typedef vector< vector<EcalWeight> > EcalWeightMatrix;
    EcalWeightMatrix& mat1 = wgt->getWeightsBeforeGainSwitch();
    EcalWeightMatrix& mat2 = wgt->getWeightsAfterGainSwitch();

    cout << "initial size of mat1: " << mat1.size() << endl;
    cout << "initial size of mat2: " << mat2.size() << endl;

    for(size_t i=0; i<3; ++i) {
      vector<EcalWeight> tv1, tv2;
      for(size_t j=0; j<10; ++j) {
	tv1.push_back( EcalWeight(i*10. + j) );
	//cout << "row: " << i << " col: " << j << " -  val: " << mat1[i][j]  << endl;
	tv2.push_back( EcalWeight(100+i*10. + j) );
      }
      mat1.push_back(tv1);
      mat2.push_back(tv2);
    }

    // fill the chi2 matrcies
    EcalWeightMatrix& mat3 = wgt->getChi2WeightsBeforeGainSwitch();
    EcalWeightMatrix& mat4 = wgt->getChi2WeightsAfterGainSwitch();
    for(size_t i=0; i<10; ++i) {
      vector<EcalWeight> tv1, tv2;
      for(size_t j=0; j<10; ++j) {
	tv1.push_back( EcalWeight(1000+i*10. + j) );
	tv2.push_back( EcalWeight(1000+100+i*10. + j) );
      }
      mat3.push_back(tv1);
      mat4.push_back(tv2);
    }

    cout << "final size of mat1: " << mat1.size() << endl;
    cout << "final size of mat2: " << mat2.size() << endl;
    cout << "Finished Building." << endl;

    cout << "Marking Weights..." << flush;
    string wgttok = wgtWriter.markWrite<EcalWeightRecAlgoWeights>(wgt);
    cout << "Done." << endl;

    cout << "Assigning IOV..." << flush;
    wgt_iov->iov.insert(make_pair(edm::IOVSyncValue::endOfTime().eventID().run(), wgttok));
    string wgtiovToken = iovWriter.markWrite<cond::IOV>(wgt_iov);
    cout << "Done." << endl;
    
    cout << "Weights written into DB with IOV" << endl;

    // Write EcalPedestals
    cout << "Building EcalPedestals." << endl;
    cond::IOV* pediov = new cond::IOV;
    int channelId;
    EcalPedestals::Item item;

    int nMaxEta = 85;
    int nMaxPhi = 20;
    EcalPedestals* ped1 = new EcalPedestals;
    for(int iEta=1; iEta<=nMaxEta;++iEta) {
      for(int iPhi=1; iPhi<=nMaxPhi; ++iPhi) {
	channelId= (iEta-1)*nMaxPhi+iPhi;
	int tt = channelId;
	if(tt%2 == 0) {
	  item.mean_x1  =0.91;
	  item.rms_x1   =0.17;
	  item.mean_x6  =0.52;
	  item.rms_x6   =0.03;
	  item.mean_x12 =0.16;
	  item.rms_x12  =0.05;
	} else {
	  item.mean_x1  =0.50;
	  item.rms_x1   =0.94;
	  item.mean_x6  =0.72;
	  item.rms_x6   =0.07;
	  item.mean_x12 =0.87;
	  item.rms_x12  =0.07;
	}
	// make an EBDetId since we need EBDetId::rawId() to be used as the key for the pedestals
	EBDetId ebdetid(iEta,iPhi);
	ped1->m_pedestals.insert(make_pair(ebdetid.rawId(),item));
      }
    }

    cout << "Finished Building." << endl;

    cout << "Marking EcalPedestals..." << flush;
    string ped1tok = pedWriter.markWrite<EcalPedestals>(ped1);
    cout << "Done." << endl;

    cout << "Assigning IOV..." << flush;
    int tillrun=73000;
    pediov->iov.insert(make_pair(tillrun,ped1tok));
    cout << "Done." << endl;

    cout << "Building more EcalPedestals." << endl;
    EcalPedestals* ped2=new EcalPedestals; //the user gives up the object ownership upon send it to the writer
    for(int iEta=1; iEta<=85;++iEta) {
      for(int iPhi=1; iPhi<=20; ++iPhi) {
	channelId= (iEta-1)*nMaxPhi+iPhi;
	int tt = channelId;
	if(tt%2 == 0) {
	  item.mean_x1=0.33;
	  item.rms_x1=0.44;
	  item.mean_x6 =0.22;
	  item.rms_x6  =0.11;
	  item.mean_x12=0.39;
	  item.rms_x12 =0.12;
	} else {
	  item.mean_x1=0.56;
	  item.rms_x1=0.98;
	  item.mean_x6 =0.83;
	  item.rms_x6  =0.27;
	  item.mean_x12=0.54;
	  item.rms_x12 =0.27;
	}

	// make an EBDetId since we need EBDetId::rawId() to be used as the key for the pedestals
	EBDetId ebdetid(iEta,iPhi);
	ped2->m_pedestals.insert(make_pair(ebdetid.rawId(),item));
      }
    }
    cout << "Finished Building" << endl;

    cout << "Marking EcalPedestals..." << flush;
    string pedtok2 = pedWriter.markWrite<EcalPedestals>(ped2);
    cout << "Done." << endl;

    cout << "Assigning IOV..." << flush;
    tillrun=75000;
    pediov->iov.insert(make_pair(tillrun,pedtok2));
    string pediovToken = pedWriter.markWrite<cond::IOV>(pediov);
    cout << "Done." << endl;

    cout << "Pedestals written into DB with IOV" << endl;

    cout << "Committing Session..." << flush;
    session->commit();
    cout << "Done." << endl;

    // Disconnect
    cout << "Disconnection Session..." << flush;
    session->disconnect();
    delete session;
    cout << "Done." << endl;
  
    cout << "Registering to MetaData Service..." << flush;
    metadata_svc->addMapping("EcalWeightRecAlgoWeights_h4_sm5", wgtiovToken);
    metadata_svc->addMapping("EcalPedestals_2008_test", pediovToken);
    cout << "Done." << endl;

    cout << "Disconnecting MetaStata Service..." << flush;
    metadata_svc->disconnect();
    delete metadata_svc;
    cout << "Done." << endl;

    delete loader;

    cout << "All Done." << endl;
  } catch(cond::Exception &e) {
    cout << e.what() << endl;
  } catch(seal::Exception &e) {
    cout << e.what() << endl;
  } catch(...) {
    cout << "Unknown exception" << endl;
  }
}
