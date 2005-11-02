#include "CondCore/DBCommon/interface/DBWriter.h"
#include "CondCore/IOVService/interface/IOV.h"
#include "CondCore/MetaDataService/interface/MetaData.h"
#include "FWCore/Framework/interface/IOVSyncValue.h"
#include "SealKernel/Service.h"
#include "POOLCore/POOLContext.h"
#include "SealKernel/Context.h"
#include <string>
#include <map>
#include <iostream>
#include <fstream>
#include <cstdio>

#include "CondFormats/EcalObjects/interface/EcalPedestals.h"
#include "CondFormats/EcalObjects/interface/EcalWeightRecAlgoWeights.h"
#include "CondFormats/EcalObjects/interface/EcalWeight.h"

using namespace std;

int main(){
  //std::string contact("oracle://devdb10/cms_xiezhen_dev");
  std::string contact("sqlite_file:ecalcalib.db");
  pool::POOLContext::loadComponent( "SEAL/Services/MessageService" );
  pool::POOLContext::setMessageVerbosityLevel( seal::Msg::Error );
  const std::string userNameEnv = "POOL_AUTH_USER=cms_xiezhen_dev";
  ::putenv( const_cast<char*>( userNameEnv.c_str() ) );
  const std::string passwdEnv = "POOL_AUTH_PASSWORD=xiezhen123";
  ::putenv( const_cast<char*>( passwdEnv.c_str() ) );

  cond::DBWriter w(contact);
  w.startTransaction();
  cond::IOV* wgt_iov=new cond::IOV;

  EcalWeightRecAlgoWeights* wgt=new EcalWeightRecAlgoWeights;

  typedef std::vector< std::vector<EcalWeight> > EcalWeightMatrix;
  EcalWeightMatrix& mat1 = wgt->getWeightsBeforeGainSwitch();
  EcalWeightMatrix& mat2 = wgt->getWeightsAfterGainSwitch();

  std::cout << "initial size of mat1: " << mat1.size() << std::endl;
  std::cout << "initial size of mat2: " << mat2.size() << std::endl;

  for(size_t i=0; i<3; ++i) {
    std::vector<EcalWeight> tv1, tv2;
    for(size_t j=0; j<10; ++j) {
      tv1.push_back( EcalWeight(i*10. + j) );
      //std::cout << "row: " << i << " col: " << j << " -  val: " << mat1[i][j]  << std::endl;
      tv2.push_back( EcalWeight(100+i*10. + j) );
    }
    mat1.push_back(tv1);
    mat2.push_back(tv2);
  }

  // fill the chi2 matrcies
  EcalWeightMatrix& mat3 = wgt->getChi2WeightsBeforeGainSwitch();
  EcalWeightMatrix& mat4 = wgt->getChi2WeightsAfterGainSwitch();
  for(size_t i=0; i<10; ++i) {
    std::vector<EcalWeight> tv1, tv2;
    for(size_t j=0; j<10; ++j) {
      tv1.push_back( EcalWeight(1000+i*10. + j) );
      tv2.push_back( EcalWeight(1000+100+i*10. + j) );
    }
    mat3.push_back(tv1);
    mat4.push_back(tv2);
  }


  std::string wgttok=w.write<EcalWeightRecAlgoWeights>(wgt, "EcalWeightRecAlgoWeights");

  //assign IOV
  wgt_iov->iov.insert(std::make_pair(edm::IOVSyncValue::endOfTime().eventID().run(),wgttok));

  std::string wgtiovToken=w.write<cond::IOV>(wgt_iov,"IOV");
  std::cout << "weights written into db with IOV" << std::endl;


  cond::IOV* pediov=new cond::IOV;
  int channelId;
  EcalPedestals::Item item;

  int nMaxEta = 85;
  int nMaxPhi = 20;
  EcalPedestals* ped1=new EcalPedestals;
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
      ped1->m_pedestals.insert(std::make_pair(channelId,item));
    }
  }
  std::string ped1tok=w.write<EcalPedestals>(ped1, "EcalPedestals");//pool::Ref takes the ownership of ped1
  //assign IOV
  int tillrun=73000;
  pediov->iov.insert(std::make_pair(tillrun,ped1tok));

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
      ped2->m_pedestals.insert(std::make_pair(channelId,item));
    }
  }

  std::string pedtok2=w.write<EcalPedestals>(ped2,"EcalPedestals");

  //assign IOV
  tillrun=75000;
  pediov->iov.insert(std::make_pair(tillrun,pedtok2));
  std::string pediovToken=w.write<cond::IOV>(pediov,"IOV");  
  std::cout << "pedestals written into db with IOV" << std::endl;

  w.commitTransaction();
  std::cout << "committed transaction" << std::endl;

  //register the iovToken to the metadata service
  cond::MetaData metadata_svc(contact);
  metadata_svc.addMapping("EcalWeightRecAlgoWeights_h4_sm5", wgtiovToken);
  metadata_svc.addMapping("EcalPedestals_2008_test", pediovToken);
}
