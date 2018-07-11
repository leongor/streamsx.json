/*
 * JsonWriter.h
 *
 *  Created on: Nov 23, 2015
 *      Author: leonid.gorelik
 */

#ifndef JSON_WRITER_H_
#define JSON_WRITER_H_

// Define SPL types and functions.
#include "SPL/Runtime/Function/SPLCast.h"
#include "SPL/Runtime/Function/MathFunctions.h"
#include "SPL/Runtime/Function/TimeFunctions.h"
#include <SPL/Runtime/Type/Tuple.h>

#include <iostream>
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include <streams_boost/algorithm/string/predicate.hpp>
#include <streams_boost/date_time/c_local_time_adjustor.hpp>
#include <streams_boost/date_time/posix_time/posix_time.hpp>

using namespace streams_boost::posix_time;
using namespace rapidjson;
using namespace SPL;

namespace com { namespace ibm { namespace streamsx { namespace json {

	typedef streams_boost::date_time::c_local_adjustor<ptime> cLocalAdjustor;
	typedef SPL::map<SPL::rstring,SPL::rstring> KeysMap;
	const int DefaultMaxFractionSize = 6; // apply max decimal places as std::fixed default

	struct FormatOptions {

		FormatOptions() :
			keysToReplace(), prefixToIgnore(""), maxFractionSize(DefaultMaxFractionSize), stripRoundFloats(false), tsFormat(""), tsUTC(false) {}

		template<typename Tuple>
		FormatOptions(Tuple const& tuple) :
			keysToReplace(tuple.get_keysToReplace()), prefixToIgnore(tuple.get_prefixToIgnore()),
			maxFractionSize(tuple.get_maxFractionSize() ? tuple.get_maxFractionSize() : DefaultMaxFractionSize),
			stripRoundFloats(tuple.get_stripRoundFloats()),	tsFormat(tuple.get_tsFormat()), tsUTC(tuple.get_tsUTC()) {}

		KeysMap keysToReplace;
		SPL::rstring prefixToIgnore;
		int maxFractionSize;
		bool stripRoundFloats;
		SPL::rstring tsFormat;
		bool tsUTC;
	};

	template<typename String>
	inline char const* convToChars(String const& str) { return str.c_str(); }

	template<>
	inline char const* convToChars<ustring>(ustring const& str) { return spl_cast<rstring,ustring>::cast(str).c_str(); }

	template<>
	inline char const* convToChars<ConstValueHandle>(const ConstValueHandle& valueHandle) {
		switch(valueHandle.getMetaType()) {
			case Meta::Type::BSTRING : {
				const BString & str = valueHandle;
				return str.getCString();
			}
			case Meta::Type::USTRING : {
				const ustring & str = valueHandle;
				return spl_cast<rstring,ustring>::cast(str).c_str();
			}
			default: {
				const rstring & str = valueHandle;
				return str.c_str();
			}
		}
	}

	template<typename Floating>
	inline bool checkStripFloat(Floating value, FormatOptions const& options) {
		if(options.stripRoundFloats) {
			const Floating presicion = pow(10, -options.maxFractionSize);
			return fabs(value) < (int)fabs(value) + presicion;
		}
		return false;
	}

	inline size_t getDecimalSize(rstring const& valueStr) {
		size_t dotPos = valueStr.find_first_of(".");
		return (dotPos != std::string::npos) ? dotPos : valueStr.size();
	}

	template<typename Container, typename Iterator>
	inline void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options);

	template<typename Container, typename Iterator>
	inline void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options);

	inline void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options);

	inline void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options);

	inline void writeKey(Writer<StringBuffer> & writer, std::string const& attrName, FormatOptions const& options) {

		typename KeysMap::const_iterator iter = options.keysToReplace.find(attrName);
		if(iter != options.keysToReplace.end())	// search key to replace in the options map
			writer.Key(convToChars( iter->second));

		else if(!options.prefixToIgnore.empty() && starts_with(attrName, options.prefixToIgnore)) // check if key is prefixed
			writer.Key(convToChars(replace_first_copy(attrName, options.prefixToIgnore, "")));

		else
			writer.Key(convToChars(attrName));
	}

	inline void writeAny(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options) {

		switch (valueHandle.getMetaType()) {
			case Meta::Type::LIST : {
				writeArray<List,ConstListIterator>(writer, valueHandle, options);
				break;
			}
			case Meta::Type::BLIST : {
				writeArray<BList,ConstListIterator>(writer, valueHandle, options);
				break;
			}
			case Meta::Type::SET : {
				writeArray<Set,ConstSetIterator>(writer, valueHandle, options);
				break;
			}
			case Meta::Type::BSET : {
				writeArray<BSet,ConstSetIterator>(writer, valueHandle, options);
				break;
			}
			case Meta::Type::MAP :
				writeMap<Map,ConstMapIterator>(writer, valueHandle, options);
				break;
			case Meta::Type::BMAP : {
				writeMap<BMap,ConstMapIterator>(writer, valueHandle, options);
				break;
			}
			case Meta::Type::TUPLE : {
				writeTuple(writer, valueHandle, options);
				break;
			}
			default:
				writePrimitive(writer, valueHandle, options);
		}
	}

	template<typename Container, typename Iterator>
	inline void writeArray(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options) {

		writer.StartArray();

		const Container & array = valueHandle;
		for(Iterator arrayIter = array.getBeginIterator(); arrayIter != array.getEndIterator(); arrayIter++) {

			const ConstValueHandle & arrayValueHandle = *arrayIter;
			writeAny(writer, arrayValueHandle, options);
		}

		writer.EndArray();
	}

	template<typename Container, typename Iterator>
	inline void writeMap(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options) {

		writer.StartObject();

		const Container & map = valueHandle;
		for(Iterator mapIter = map.getBeginIterator(); mapIter != map.getEndIterator(); mapIter++) {

			const std::pair<ConstValueHandle,ConstValueHandle> & mapHandle = *mapIter;
			const ConstValueHandle & mapValueHandle = mapHandle.second;

			writer.Key(convToChars(mapHandle.first));
			writeAny(writer, mapValueHandle, options);
		}

		writer.EndObject();
	}

	inline void writeTuple(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options) {
		using namespace streams_boost::algorithm;

		writer.StartObject();

		const Tuple & tuple = valueHandle;
		for(ConstTupleIterator tupleIter = tuple.getBeginIterator(); tupleIter != tuple.getEndIterator(); tupleIter++) {

			const std::string & attrName = (*tupleIter).getName();
			const ConstValueHandle & attrValueHandle = static_cast<ConstTupleAttribute>(*tupleIter).getValue();

			writeKey(writer, attrName, options);
			writeAny(writer, attrValueHandle, options);
		}

		writer.EndObject();
	}

	inline void writePrimitive(Writer<StringBuffer> & writer, ConstValueHandle const& valueHandle, FormatOptions const& options) {

		switch (valueHandle.getMetaType()) {
			case Meta::Type::BOOLEAN : {
				const boolean & value = valueHandle;
				writer.Bool(value);
				break;
			}
			case Meta::Type::ENUM : {
				const Enum & value = valueHandle;
				writer.String(convToChars(value.getValue()));
				break;
			}
			case Meta::Type::INT8 : {
				const int8 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT16 : {
				const int16 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT32 : {
				const int32 & value = valueHandle;
				writer.Int(value);
				break;
			}
			case Meta::Type::INT64 : {
				const int64 & value = valueHandle;
				writer.Int64(value);
				break;
			}
			case Meta::Type::UINT8 : {
				const uint8 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT16 : {
				const uint16 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT32 : {
				const uint32 & value = valueHandle;
				writer.Uint(value);
				break;
			}
			case Meta::Type::UINT64 : {
				const uint64 & value = valueHandle;
				writer.Uint64(value);
				break;
			}
			case Meta::Type::FLOAT32 : {
				const float32 & value = valueHandle;
				if(checkStripFloat(value, options))
					writer.Int(value);
				else
					writer.Double(value);
				break;
			}
			case Meta::Type::FLOAT64 : {
				const float64 & value = valueHandle;
				if(checkStripFloat(value, options))
					writer.Int64(value);
				else
					writer.Double(value);
				break;
			}
			case Meta::Type::DECIMAL32 : {
				const decimal32 & value = valueHandle;
				const rstring & valueStr = spl_cast<rstring,decimal32>::cast(value);
				size_t valueSize = (options.stripRoundFloats && value == Functions::Math::floor(value)) ? getDecimalSize(valueStr) : valueStr.size();
				writer.RawValue(convToChars(valueStr), valueSize, kNumberType);
				break;
			}
			case Meta::Type::DECIMAL64 : {
				const decimal64 & value = valueHandle;
				const rstring & valueStr = spl_cast<rstring,decimal64>::cast(value);
				size_t valueSize = (options.stripRoundFloats && value == Functions::Math::floor(value)) ? getDecimalSize(valueStr) : valueStr.size();
				writer.RawValue(convToChars(valueStr), valueSize, kNumberType);
				break;
			}
			case Meta::Type::DECIMAL128 : {
				const decimal128 & value = valueHandle;
				const rstring & valueStr = spl_cast<rstring,decimal128>::cast(value);
				size_t valueSize = (options.stripRoundFloats && value == Functions::Math::floor(value)) ? getDecimalSize(valueStr) : valueStr.size();
				writer.RawValue(convToChars(valueStr), valueSize, kNumberType);
				break;
			}
			case Meta::Type::COMPLEX32 : {
				writer.Null();
				break;
			}
			case Meta::Type::COMPLEX64 : {
				writer.Null();
				break;
			}
			case Meta::Type::TIMESTAMP : {
				const timestamp & value = valueHandle;

				if(options.tsFormat.empty()) {
					writer.String(convToChars(Functions::Time::ctime(value)));
				}
				else {
					std::stringstream stream;
					stream.imbue(std::locale(std::locale::classic(), new time_facet(options.tsFormat.c_str()))); // time_facet pointer is managed by std::locale::facet
					const ptime dateTime( ptime( from_time_t(value.getSeconds())) + microseconds(value.getNanoseconds() / 1000));
					if(options.tsUTC)
						stream << dateTime << 'Z';
					else
						stream << cLocalAdjustor::utc_to_local(dateTime);

					writer.String(convToChars(stream.str()));
				}
				break;
			}
			case Meta::Type::BSTRING :
			case Meta::Type::RSTRING :
			case Meta::Type::USTRING : {
				writer.String(convToChars(valueHandle));
				break;
			}
			case Meta::Type::BLOB : {
				writer.Null();
				break;
			}
			case Meta::Type::XML : {
				writer.Null();
				break;
			}
			default:
				writer.Null();
		}
	}


	template<typename Composite>
	inline SPL::rstring toJSONImpl(Composite const& comp, FormatOptions const& options) {

	    StringBuffer s;
	    Writer<StringBuffer> writer(s);
	    writer.SetMaxDecimalPlaces(options.maxFractionSize);

		writeAny(writer, ConstValueHandle(comp), options);

		return s.GetString();
	}

	template<typename String, typename SPLAny>
	inline SPL::rstring toJSONImpl(String const& key, SPLAny const& splAny, FormatOptions const& options) {


	    StringBuffer s;
	    Writer<StringBuffer> writer(s);
	    writer.SetMaxDecimalPlaces(options.maxFractionSize);

		writer.StartObject();

		writer.Key(convToChars(key));

		writeAny(writer, ConstValueHandle(splAny), options);

		writer.EndObject();

		return s.GetString();
	}

}}}}

#endif /* JSON_WRITER_H_ */

namespace com { namespace ibm { namespace streamsx { namespace json {

	namespace { // this anonymous namespace will be defined for each operator separately

		inline FormatOptions const& getFormatOptions(FormatOptions const* fo = NULL) {

			static const FormatOptions _fo(fo ? *fo : FormatOptions());
			return _fo;
		}

		template<typename Tuple>
		inline bool setFormatOptions(Tuple const& options) {

			const FormatOptions fo(options);
			getFormatOptions(&fo);
			return true;
		}


		inline SPL::rstring tupleToJSON(Tuple const& tuple) {

			return toJSONImpl(tuple, getFormatOptions());
		}

		template<typename MAP>
		inline SPL::rstring mapToJSON(MAP const& map) {

			return toJSONImpl(map, getFormatOptions());
		}

		template<typename String, typename SPLAny>
		inline SPL::rstring toJSON(String const& key, SPLAny const& splAny) {

			return toJSONImpl(key, splAny, getFormatOptions());
		}

		template<typename String, typename SPLAny, typename KeysMap>
		inline SPL::rstring toJSON(String const& key, SPLAny const& splAny) {

			return toJSONImpl(key, splAny, getFormatOptions());
		}

	}

}}}}
