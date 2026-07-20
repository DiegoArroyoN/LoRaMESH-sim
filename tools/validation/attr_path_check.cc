#include "ns3/core-module.h"
#include "ns3/dv-cl-app.h"
#include "ns3/dv-cl-helper.h"
#include "ns3/dv-cl-lora-net-device.h"
#include "ns3/dv-cl-mac-csma-cad.h"
#include "ns3/dv-cl-regional-profile.h"
#include "ns3/dv-cl-routing.h"
#include <fstream>
#include <iostream>
#include <string>
using namespace ns3;
int main(int argc, char* argv[])
{
    std::string file = (argc > 1) ? argv[1] : "/tmp/paths.txt";
    std::ifstream in(file);
    std::string path;
    int ok = 0, badType = 0, badAttr = 0;
    while (std::getline(in, path))
    {
        if (path.empty()) continue;
        auto pos = path.rfind("::");
        if (pos == std::string::npos) { std::cout << "MALFORMED " << path << "\n"; continue; }
        std::string tidName = path.substr(0, pos);
        std::string attrName = path.substr(pos + 2);
        TypeId tid;
        if (!TypeId::LookupByNameFailSafe(tidName, &tid))
        {
            std::cout << "TIPO-INEXISTENTE  " << path << "\n"; ++badType; continue;
        }
        TypeId::AttributeInformation info;
        if (!tid.LookupAttributeByName(attrName, &info))
        {
            std::cout << "ATRIBUTO-INEXISTENTE  " << path << "\n"; ++badAttr; continue;
        }
        ++ok;
    }
    std::cout << "\nRESUMEN: ok=" << ok << " tipo_inexistente=" << badType
              << " atributo_inexistente=" << badAttr << "\n";
    return (badType + badAttr) ? 1 : 0;
}
