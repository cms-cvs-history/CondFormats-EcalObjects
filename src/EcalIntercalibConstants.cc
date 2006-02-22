#include "CondFormats/EcalObjects/interface/EcalIntercalibConstants.h"

EcalIntercalibConstants::EcalIntercalibConstants() {
}

EcalIntercalibConstants::~EcalIntercalibConstants() {

}

void
EcalIntercalibConstants::setValue(const uint32_t& id, const EcalIntercalibConstant & value) {
  map_[id] = value;
}
