#include "config_reader.h"

#include <fstream>
#include <iostream>
#include <sstream>

std::string trim(std::string const& source, char const* delims = " \t\r\n") {
  std::string result(source);
  std::string::size_type index = result.find_last_not_of(delims);
  if(index != std::string::npos)
    result.erase(++index);

  index = result.find_first_not_of(delims);
  if(index != std::string::npos)
    result.erase(0, index);
  else
    result.erase();
  return result;
}

map<string, string> ConfigReader::content_;

void ConfigReader::Initialize(std::string const& ConfigReader) {
  std::ifstream file(ConfigReader.c_str());

  std::string line;
  std::string name;
  std::string value;
  int posEqual;
  while (std::getline(file,line)) {

    if (! line.length()) continue;

    if (line[0] == '#') continue;
    if (line[0] == ';') continue;


    posEqual=line.find('=');
    name  = trim(line.substr(0,posEqual));
    value = trim(line.substr(posEqual+1));

    std::ostringstream strs;
    strs << value;
    std::string str = strs.str();
    ConfigReader::content_[name]=str;
  }
}

string ConfigReader::Value(std::string const& entry) {

  map<string, string>::const_iterator ci = ConfigReader::content_.find(entry);

  if (ci == ConfigReader::content_.end()) throw "does not exist";

  return ci->second;
}
//
//string ConfigReader::Value(std::string const& section, std::string const& entry, double value) {
//  try {
//    return Value(section, entry);
//  } catch(const char *) {
//	  std::ostringstream strs;
//	  strs << dbl;
//	  std::string str = strs.str();
//    return content_.insert(make_pair(section+'/'+entry, itoa(value))).first->second;
//  }
//}
//
//string ConfigReader::Value(std::string const& section, std::string const& entry, std::string const& value) {
//  try {
//    return Value(section, entry);
//  } catch(const char *) {
//    return content_.insert(make_pair(section+'/'+entry, itoa(value))).first->second;
//  }
//}
