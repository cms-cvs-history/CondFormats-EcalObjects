#include "CondFormats/EcalObjects/interface/EcalErrorDictionary.h"

#include <iostream>

int main(int argc, char* argv[]) {
  std::cout << "Errors for 3" << std::endl;
  EcalErrorDictionary::printErrors(3);
  std::cout << std::endl;

  std::cout << "Getting bitmask for PEDESTAL_MEAN_AMPLITUDE_TOO_LOW" << std::endl;
  std::cout << "bitmask:  " << EcalErrorDictionary::getMask("PEDESTAL_MEAN_AMPLITUDE_TOO_LOW") << std::endl;
  std::cout << std::endl;

  std::cout << "Checking whether '3' has the problem 'PEDESTAL_MEAN_AMPLITUDE_TOO_LOW'" << std::endl;
  std::cout << EcalErrorDictionary::hasError("PEDESTAL_MEAN_AMPLITUDE_TOO_LOW", 3) << std::endl;
  std::cout << std::endl;
}
