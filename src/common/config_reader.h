/*
 * config_reader.h
 *
 *  Created on: 14/04/2017
 *      Author: li
 */
#include <map>

#ifndef COMMON_CONFIG_READER_H_
#define COMMON_CONFIG_READER_H_

using namespace std;

class ConfigReader {
public:
	static map<string, string> content_;

public:
	static void Initialize(std::string const& configFile);

	static string Value(std::string const& entry);

	//static string Value(std::string const& section, std::string const& entry, double value);
	//static string Value(std::string const& section, std::string const& entry, std::string const& value);
};

#endif /* COMMON_CONFIG_READER_H_ */
