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
#include "CondFormats/EcalObjects/interface/EcalDCUTemperatures.h"
#include "CondFormats/EcalObjects/interface/EcalPTMTemperatures.h"
#include "CondFormats/EcalObjects/interface/EcalChannelStatus.h"
#include "CondFormats/EcalObjects/interface/EcalMonitoringCorrections.h"
#include "CondFormats/EcalObjects/interface/EcalWeightRecAlgoWeights.h"
#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "DataFormats/EcalDetId/interface/EBDetId.h"

using namespace std;

int main(){

  cout << "Setting up DB Connection..." << flush;
  cond::ServiceLoader* loader = new cond::ServiceLoader;
  loader->loadMessageService(cond::Error);
  //  loader->loadAuthenticationService(cond::XML);  // unneccessary?

  string contact("sqlite_file:ecalobjects.db");
  cond::DBSession* session = new cond::DBSession(contact);
  cond::MetaData* metadata_svc = new cond::MetaData(contact, *loader);
  cout << "Done." << endl;

try {
  cout << "Making Connections..." << flush;
  session->connect(cond::ReadWriteCreate);
  metadata_svc->connect();
  cout << "Done." << endl;

  cout << "Creating Writers..." << flush;
  cond::DBWriter grpWriter(*session, "EcalWeightXtalGroups");
  cond::DBWriter tbwgtWriter(*session, "EcalTBWeights");
  cond::DBWriter agcWriter(*session, "EcalADCToGeVConstant");
  cond::DBWriter grWriter(*session, "EcalGainRatios");
  cond::DBWriter icalWriter(*session, "EcalIntercalibConstants");
  cond::DBWriter pedWriter(*session, "EcalPedestals");
  cond::DBWriter wgtWriter(*session, "EcalWeightRecAlgoWeights");
  cond::DBWriter dcuTWriter(*session, "EcalDCUTemperatures");
  cond::DBWriter ptmTWriter(*session, "EcalPTMTemperatures");
  cond::DBWriter chStWriter(*session, "EcalChannelStatus");
  cond::DBWriter monCorWriter(*session, "EcalMonitoringCorrections");

  cond::DBWriter iovWriter(*session, "IOV");
  cout << "Done." << endl;

  cout << "Starting Transaction." << endl;
  session->startUpdateTransaction();


  size_t nMaxEta = 85;
  size_t nMaxPhi = 20;

  // create groups of crystals
  EcalWeightXtalGroups* xtalGroups = new EcalWeightXtalGroups();
  for(size_t ieta=1; ieta<=nMaxEta; ++ieta) {
    for(size_t iphi=1; iphi<=nMaxPhi; ++iphi) {
      EBDetId ebid(ieta,iphi);
      xtalGroups->setValue(ebid.rawId(), EcalXtalGroupId(ieta) ); // define rings in eta
    }
  }
  std::string grp_tok = grpWriter.markWrite<EcalWeightXtalGroups>(xtalGroups);

  // iov for groups of xtals
  cond::IOV* grp_iov = new cond::IOV;
  grp_iov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),grp_tok));
  std::string grp_iov_Token = iovWriter.markWrite<cond::IOV>(grp_iov);
  std::cout << "Map of xtals put in the DB with IoV" << std::endl;


  // create weights for the test-beam
  EcalTBWeights* tbwgt = new EcalTBWeights();

  // create weights for each distinct group ID
  size_t nMaxTDC = 10;
  for(size_t igrp=1; igrp<=nMaxEta; ++igrp) {
    for(size_t itdc=1; itdc<=nMaxTDC; ++itdc) {
      // generate random number
      double r = (double)std::rand()/( double(RAND_MAX)+double(1) );

      // make a new set of weights
      EcalWeightSet wgt;
      //typedef std::vector< std::vector<EcalWeight> > EcalWeightSet::EcalWeightMatrix;
      EcalWeightSet::EcalWeightMatrix& mat1 = wgt.getWeightsBeforeGainSwitch();
      EcalWeightSet::EcalWeightMatrix& mat2 = wgt.getWeightsAfterGainSwitch();

      //std::cout << "initial size of mat1: " << mat1.size() << std::endl;
      //std::cout << "initial size of mat2: " << mat2.size() << std::endl;

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

      cout << "group: " << igrp << " TDC: " << itdc 
           << " mat1: " << mat1.size() << " mat2: " << mat2.size()
           << " mat3: " << mat3.size() << " mat4: " << mat4.size()
           << endl;

      // put the weight in the container
      //tbwgt->setValue(igrp,itdc, wgt);
      tbwgt->setValue(std::make_pair(igrp,itdc), wgt);
      cout << "size of EcalTBWeightsMap: " << tbwgt->getMap().size() << endl;
    }
  }

  // create and store IOV for EcalTBWeights
  std::string tbwgt_tok = tbwgtWriter.markWrite<EcalTBWeights>(tbwgt);
  cond::IOV* tbwgt_iov =new cond::IOV;
  tbwgt_iov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),tbwgt_tok));
  std::string tbwgt_iov_Token = iovWriter.markWrite<cond::IOV>(tbwgt_iov);
  std::cout << "Weights for the TB put in DB with IoV" << std::endl;

  // create ADC -> GeV scale
  EcalADCToGeVConstant* agc1 = new EcalADCToGeVConstant(37.,60.);
  std::string agc1_tok = agcWriter.markWrite<EcalADCToGeVConstant>(agc1);

  // new iov object
  cond::IOV* agc_iov =new cond::IOV;
  agc_iov->iov.insert(std::make_pair(72000,agc1_tok)); // run number

  // new ADC scale for new IOV
  EcalADCToGeVConstant* agc2 = new EcalADCToGeVConstant(36.,61.);
  std::string agc2_tok = agcWriter.markWrite<EcalADCToGeVConstant>(agc2);
  agc_iov->iov.insert(std::make_pair(73000,agc2_tok)); // run number

  // yet another ADC scale with new IOV
  EcalADCToGeVConstant* agc3 = new EcalADCToGeVConstant(39.,64.);
  std::string agc3_tok = agcWriter.markWrite<EcalADCToGeVConstant>(agc3);
  agc_iov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),agc3_tok)); // run number

  std::string agc_iov_Token = iovWriter.markWrite<cond::IOV>(agc_iov);
  std::cout << "ADC->GeV scale stored in DB with IoV" << std::endl;


  // create inter calib constants
  EcalIntercalibConstants* ical = new EcalIntercalibConstants;
  EcalDCUTemperatures* dcuTemp = new EcalDCUTemperatures;
  EcalPTMTemperatures* ptmTemp = new EcalPTMTemperatures;
  EcalChannelStatus* chStatus = new EcalChannelStatus;
  EcalMonitoringCorrections* monCorr = new EcalMonitoringCorrections;

  // create gain ratios
  EcalGainRatios* gratio = new EcalGainRatios;

  for(size_t ieta=1; ieta<=nMaxEta; ++ieta) {
    for(size_t iphi=1; iphi<=nMaxPhi; ++iphi) {

      EBDetId ebid(ieta,iphi);
      //EcalXtalGroupsMap::const_iterator  it =  xtalGroups->getMap().find(ebid.rawId());
      //EcalXtalGroupId gid = (*it).second;
      //EcalXtalGroupId gId =  ( *(xtalGroups->getMap().find(ebid.rawId())) ).second;

      double r = (double)std::rand()/( double(RAND_MAX)+double(1) );
      ical->setValue( ebid.rawId(), 0.85 + r*0.3 );
      
      dcuTemp->setValue( ebid.rawId(), 20. + ebid.hashedIndex()*0.1);
      ptmTemp->setValue( ebid.rawId(), 20. + ebid.hashedIndex()*0.5);
      chStatus->setValue( ebid.rawId(),  EcalChannelStatusCode(ebid.hashedIndex()) );
      monCorr->setValue( ebid.rawId(), 1. + ebid.hashedIndex()*0.1 );
      EcalMGPAGainRatio gr;
      gr.setGain12Over6( 1.9 + r*0.2 );
      gr.setGain6Over1( 5.9 + r*0.2 );

      gratio->setValue( ebid.rawId(), gr );

    } // loop over phi
  } // loop over eta

  std::string gr_tok = grWriter.markWrite<EcalGainRatios>(gratio);//pool::Ref takes the ownership of ped1
  std::string ical_tok = icalWriter.markWrite<EcalIntercalibConstants>(ical);//pool::Ref takes the ownership of ped1
  std::string dcuT_tok = dcuTWriter.markWrite<EcalDCUTemperatures>(dcuTemp);//pool::Ref takes the ownership of ped1
  std::string ptmT_tok = ptmTWriter.markWrite<EcalPTMTemperatures>(ptmTemp);//pool::Ref takes the ownership of ped1
  std::string chStatus_tok = chStWriter.markWrite<EcalChannelStatus>(chStatus);//pool::Ref takes the ownership of ped1
  std::string monCorr_tok = monCorWriter.markWrite<EcalMonitoringCorrections>(monCorr);//pool::Ref takes the ownership of ped1

  // create IOV for EcalGainRatios
  cond::IOV* gr_iov=new cond::IOV;
  gr_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),gr_tok) );
  std::string gr_iov_Token = iovWriter.markWrite<cond::IOV>(gr_iov);
  std::cout << "Gain ratios stored in db with IoV" << std::endl;

  // create IOV for EcalIntercalibConstants
  cond::IOV* ical_iov=new cond::IOV;
  ical_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),ical_tok) );
  std::string ical_iov_Token = iovWriter.markWrite<cond::IOV>(ical_iov);
  std::cout << "Intercalib constants stored in DB with IoV" << std::endl;

  cond::IOV* dcuT_iov=new cond::IOV;
  dcuT_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),dcuT_tok) );
  std::string dcuT_iov_Token = iovWriter.markWrite<cond::IOV>(dcuT_iov);
  std::cout << "DCU Temp stored in db with IoV" << std::endl;

  cond::IOV* ptmT_iov=new cond::IOV;
  ptmT_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),ptmT_tok) );
  std::string ptmT_iov_Token = iovWriter.markWrite<cond::IOV>(ptmT_iov);
  std::cout << "PTM Temp stored in db with IoV" << std::endl;

  cond::IOV* chStatus_iov=new cond::IOV;
  chStatus_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),chStatus_tok) );
  std::string chStatus_iov_Token = iovWriter.markWrite<cond::IOV>(chStatus_iov);
  std::cout << "Channel Status stored in db with IoV" << std::endl;

  cond::IOV* monCorr_iov=new cond::IOV;
  monCorr_iov->iov.insert( std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),monCorr_tok) );
  std::string monCorr_iov_Token = iovWriter.markWrite<cond::IOV>(monCorr_iov);
  std::cout << "Monitoring corrections stored in db with IoV" << std::endl;


  // create EcalPedestals
  EcalPedestals* ped1=new EcalPedestals;
  EcalPedestals::Item item;
  int channelId;
  for(size_t iEta=1; iEta<=nMaxEta;++iEta) {
    for(size_t iPhi=1; iPhi<=nMaxPhi; ++iPhi) {
      channelId= (iEta-1)*nMaxPhi+iPhi;
      size_t tt = channelId;
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
      ped1->m_pedestals.insert(std::make_pair(ebdetid.rawId(),item));
    }
  }

  std::string ped1tok = pedWriter.markWrite<EcalPedestals>(ped1);//pool::Ref takes the ownership of ped1

  // create IOV for EcalPedestals - NB: only ONE iov object needed for any number of iov's to store
  cond::IOV* pediov=new cond::IOV;

  // insert first iov in to iov object
  int tillrun=73000;
  pediov->iov.insert(std::make_pair(tillrun,ped1tok));

  // new pedestals with new IoV
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
      //std::cout << "iphi: " << iPhi << " ieta: " << iEta << " channel: " << channelId << endl;
      // make an EBDetId since we need EBDetId::rawId() to be used as the key for the pedestals
      EBDetId ebdetid(iEta,iPhi);
      ped2->m_pedestals.insert(std::make_pair(ebdetid.rawId(),item));
    }
  }

  std::string pedtok2 = pedWriter.markWrite<EcalPedestals>(ped2);

  // insert new iov for new set of pedestals in the same IOV object
  tillrun=75000;
  pediov->iov.insert(std::make_pair(tillrun,pedtok2));
  std::string pediovToken = iovWriter.markWrite<cond::IOV>(pediov);
  std::cout << "pedestals written into db with IOV" << std::endl;

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

    cout << "Marking Weights..." << endl;
    string wgttok = wgtWriter.markWrite<EcalWeightRecAlgoWeights>(wgt);
    cout << "Done." << endl;

    cout << "Assigning IOV..." << endl;
    cond::IOV* wgt_iov = new cond::IOV;
    wgt_iov->iov.insert(make_pair(edm::IOVSyncValue::endOfTime().eventID().run(), wgttok));
    string wgtiovToken = iovWriter.markWrite<cond::IOV>(wgt_iov);
    cout << "Done." << endl;
    cout << "Weights written into DB with IOV" << endl;

  cout << "Committing Session..." << endl;
  session->commit();
  cout << "Done." << endl;

  // Disconnect
  cout << "Disconnection Session..." << endl;
  session->disconnect();
  delete session;
  cout << "Done." << endl;


  cout << "Registering to MetaData Service..." << flush;
  metadata_svc->addMapping("EcalWeightXtalGroups", grp_iov_Token);
  metadata_svc->addMapping("EcalTBWeights", tbwgt_iov_Token);
  metadata_svc->addMapping("EcalPedestals", pediovToken);
  metadata_svc->addMapping("EcalGainRatios", gr_iov_Token);
  metadata_svc->addMapping("EcalIntercalibConstants", ical_iov_Token);
  metadata_svc->addMapping("EcalADCToGeVConstant", agc_iov_Token);
  metadata_svc->addMapping("EcalWeightRecAlgoWeights", wgtiovToken);
  metadata_svc->addMapping("EcalDCUTemperatures", dcuT_iov_Token);
  metadata_svc->addMapping("EcalPTMTemperatures", ptmT_iov_Token);
  metadata_svc->addMapping("EcalChannelStatus", chStatus_iov_Token);
  metadata_svc->addMapping("EcalMonitoringCorrections", monCorr_iov_Token);
  cout << "Done." << endl;

  cout << "Disconnecting MetaStata Service..." << flush;
  metadata_svc->disconnect();
  delete metadata_svc;
  cout << "Done." << endl;

  delete loader;

} catch(cond::Exception &e) {
  cout << e.what() << endl;
} catch(...) {
  cout << "Unknown exception" << endl;
}

}
