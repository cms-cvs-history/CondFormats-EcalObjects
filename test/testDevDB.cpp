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

#include "CondFormats/EcalObjects/interface/EcalWeightXtalGroups.h"
#include "CondFormats/EcalObjects/interface/EcalXtalGroupId.h"
#include "CondFormats/EcalObjects/interface/EcalTBWeights.h"
#include "CondFormats/EcalObjects/interface/EcalWeightSet.h"
#include "CondFormats/EcalObjects/interface/EcalWeight.h"
#include "CondFormats/EcalObjects/interface/EcalIntercalibConstants.h"
#include "CondFormats/EcalObjects/interface/EcalGainRatios.h"
#include "CondFormats/EcalObjects/interface/EcalMGPAGainRatio.h"
#include "CondFormats/EcalObjects/interface/EcalADCToGeVConstant.h"

#include "CondFormats/EcalObjects/interface/EcalWeightRecAlgoWeights.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"

using namespace std;

//-------------------------------------------------------------
class Application {
//-------------------------------------------------------------
  private:
   cond::ServiceLoader* loader;
   cond::DBSession*     session;
   cond::MetaData*      metadata_svc;
   cond::DBWriter* grpWriter;
   cond::DBWriter* tbwgtWriter;
   cond::DBWriter* agcWriter;
   cond::DBWriter* grWriter;
   cond::DBWriter* icalWriter;
   cond::DBWriter* pedWriter;
   cond::DBWriter* wgtWriter;
   cond::DBWriter* iovWriter;

   std::string tag_;

  private:
    EcalWeightXtalGroups* generateEcalWeightXtalGroups();
    EcalTBWeights*        generateEcalTBWeights();
    EcalADCToGeVConstant* generateEcalADCToGeVConstant();
    EcalIntercalibConstants* generateEcalIntercalibConstants();
    EcalGainRatios*       generateEcalGainRatios();
    EcalPedestals*        generateEcalPedestals();

  public:
    Application(const std::string& connection,  const std::string& tag);
    ~Application();
    void writeObjectsIntoDB(unsigned long firstrun, unsigned long lastrun);
};
//-------------------------------------------------------------
//-------------------------------------------------------------

//-------------------------------------------------------------
Application::Application(const std::string& connection, const std::string& tag) {
//-------------------------------------------------------------

  tag_ = tag;

  cout << "Setting up DB Connection..." << flush;
  loader = new cond::ServiceLoader;
  loader->loadMessageService(cond::Error);

  session = new cond::DBSession(connection);
  session->connect(cond::ReadWriteCreate);

  metadata_svc = new cond::MetaData(connection, *loader);
  metadata_svc->connect();

  cout << "Done." << endl;

  cout << "Creating Writers..." << flush;
  grpWriter  = new cond::DBWriter(*session, "EcalWeightXtalGroups");
  tbwgtWriter= new cond::DBWriter(*session, "EcalTBWeights");
  agcWriter  = new cond::DBWriter(*session, "EcalADCToGeVConstant");
  grWriter   = new cond::DBWriter(*session, "EcalGainRatios");
  icalWriter = new cond::DBWriter(*session, "EcalIntercalibConstants");
  pedWriter  = new cond::DBWriter(*session, "EcalPedestals");
  wgtWriter  = new cond::DBWriter(*session, "EcalWeightRecAlgoWeights");
  iovWriter  = new cond::DBWriter(*session, "IOV");
  cout << "Done." << endl;

}

//-------------------------------------------------------------
Application::~Application() {
//-------------------------------------------------------------

  delete loader;
  session->disconnect();
  delete session;
  metadata_svc->disconnect();
  delete metadata_svc;

  delete grpWriter  ;
  delete tbwgtWriter;
  delete agcWriter  ;
  delete grWriter   ;
  delete icalWriter ;
  delete pedWriter  ;
  delete wgtWriter  ;
  delete iovWriter  ;

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
  EcalADCToGeVConstant* agc = new EcalADCToGeVConstant(36.+r*4.);
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
void Application::writeObjectsIntoDB(unsigned long firstrun, unsigned long lastrun) {
//-------------------------------------------------------------

  //define iov objects
  cond::IOV* grp_iov = new cond::IOV;
  cond::IOV* tbwgt_iov =new cond::IOV;
  cond::IOV* agc_iov =new cond::IOV;
  cond::IOV* ical_iov=new cond::IOV;
  cond::IOV* gr_iov=new cond::IOV;
  //cond::IOV* pediov=new cond::IOV;

  for(unsigned long irun=firstrun; irun<=lastrun; ++irun) {
    // Arguments 0 0 mean infine IOV
    if (firstrun == 0 && lastrun == 0) {
      cout << "Infinite IOV mode" << endl;
      irun = edm::IOVSyncValue::endOfTime().eventID().run();
    }

    cout << "Starting Transaction for run " << irun << endl;
    session->startUpdateTransaction();

    EcalWeightXtalGroups* xtalGroups = generateEcalWeightXtalGroups();
    std::string grp_tok = grpWriter->markWrite<EcalWeightXtalGroups>(xtalGroups);
    grp_iov->iov.insert(std::make_pair(irun,grp_tok));

    EcalTBWeights* tbwgt = generateEcalTBWeights();
    std::string tbwgt_tok = tbwgtWriter->markWrite<EcalTBWeights>(tbwgt);
    tbwgt_iov->iov.insert(std::make_pair(irun,tbwgt_tok));

    EcalADCToGeVConstant* agc = generateEcalADCToGeVConstant();
    std::string agc_tok = agcWriter->markWrite<EcalADCToGeVConstant>(agc);
    agc_iov->iov.insert(std::make_pair(irun,agc_tok));

    EcalIntercalibConstants* ical = generateEcalIntercalibConstants();
    std::string ical_tok = icalWriter->markWrite<EcalIntercalibConstants>(ical);
    ical_iov->iov.insert( std::make_pair(irun,ical_tok) );

    EcalGainRatios* gratio = generateEcalGainRatios();
    std::string gr_tok = grWriter->markWrite<EcalGainRatios>(gratio);
    gr_iov->iov.insert( std::make_pair(irun,gr_tok) );

    //EcalPedestals* ped = generateEcalPedestals();
    //std::string pedtok = pedWriter->markWrite<EcalPedestals>(ped);
    //pediov->iov.insert(std::make_pair(irun,pedtok));

    cout << "Committing Session for run " << irun << " ..." << flush;
    session->commit();
    cout << "Done." << endl;

    // End loop on infinite IOV
    if (firstrun == 0 && lastrun == 0) {
      break;
    }
  }

  // writing iov's and adding mapping for meta data

  // new session to write IOV objects
  cout << "Starting transaction for IOV objects." << endl;
  session->startUpdateTransaction();

  std::string grp_iov_Token = iovWriter->markWrite<cond::IOV>(grp_iov);
  std::string tbwgt_iov_Token = iovWriter->markWrite<cond::IOV>(tbwgt_iov);
  std::string agc_iov_Token = iovWriter->markWrite<cond::IOV>(agc_iov);
  std::string ical_iov_Token = iovWriter->markWrite<cond::IOV>(ical_iov);
  std::string gr_iov_Token = iovWriter->markWrite<cond::IOV>(gr_iov);
  //std::string pediovToken = iovWriter->markWrite<cond::IOV>(pediov);

  cout << "Committing Session for IOVs..." << flush;
  session->commit();
  cout << "Done." << endl;

  // finally add mapping for meta data
  cout << "Adding metadata to objects..." << flush;

  std::string grp_tag("EcalWeightXtalGroups_"+tag_);
  metadata_svc->addMapping(grp_tag, grp_iov_Token);

  std::string tbwgt_tag("EcalTBWeights_"+tag_);
  metadata_svc->addMapping(tbwgt_tag, tbwgt_iov_Token);

  std::string agc_tag("EcalADCToGeVConstant_"+tag_);
  metadata_svc->addMapping(agc_tag, agc_iov_Token);

  std::string ical_tag("EcalIntercalibConstants_"+tag_);
  metadata_svc->addMapping(ical_tag, ical_iov_Token);

  std::string gr_tag("EcalGainRatios_"+tag_);
  metadata_svc->addMapping(gr_tag, gr_iov_Token);

  //std::string ped_tag("EcalPedestals_"+tag_);
  //metadata_svc->addMapping(tag_, pediovToken);

  cout << "Done." << endl;

} // writeObjectsIntoDB()




// =========
int main(int argc, char* argv[]){
// =========

  // usage
  if (argc != 5) {
    cout << "testDevDB: "
         << " <connection> <first run> <last run> <tag>"
         << endl;
    exit(-1);
  }

  // parse the command line arguments
  std::string connection = argv[1];
  unsigned long firstRun = (unsigned long)atoi(argv[2]);
  unsigned long lastRun = (unsigned long)atoi(argv[3]);
  std::string tag = argv[4];


  Application app(connection, tag);

  try {
    app.writeObjectsIntoDB(firstRun,lastRun);
  } catch(cond::Exception &e) {
    cout << e.what() << endl;
  } catch(seal::Exception &e) {
    cout << e.what() << endl;
  } catch(...) {
    cout << "Unknown exception" << endl;
  }

  return 0;

}
