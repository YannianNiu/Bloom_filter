/* Copyright (c) 2015 Brian R. Bondy. Distributed under the MPL2 license.
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

#include <string.h>
#include "bloom_filter.hpp"

BloomFilter::BloomFilter(unsigned int bitsPerElement,
    unsigned int estimatedNumElements, HashFunction *hashFunctions, int numHashFunctions) :
    hashFunctions(nullptr), numHashFunctions(0), byteBufferSize(0), buffer(nullptr) {
  this->hashFunctions = hashFunctions;
  this->numHashFunctions = numHashFunctions;
  lastHash = new uint64_t[numHashFunctions];
  byteBufferSize = bitsPerElement * estimatedNumElements / 8 + 1;
  bitBufferSize = byteBufferSize * 8;
  buffer = new char[byteBufferSize];
  memset(buffer, 0, byteBufferSize);
}

// Constructs a BloomFilter by copying the specified buffer and number of bytes
BloomFilter::BloomFilter(const char *buffer, int byteBufferSize,
    HashFunction *hashFunctions, int numHashFunctions) :
    hashFunctions(nullptr), numHashFunctions(0), byteBufferSize(0), buffer(nullptr) {
  this->hashFunctions = hashFunctions;
  this->numHashFunctions = numHashFunctions;
  lastHash = new uint64_t[numHashFunctions];
  this->byteBufferSize = byteBufferSize;
  bitBufferSize = byteBufferSize * 8;
  this->buffer = new char[byteBufferSize];
  memcpy(this->buffer, buffer, byteBufferSize);
}

BloomFilter::~BloomFilter() {
  if (buffer) {
    delete[] buffer;
  }
  if (lastHash) {
    delete[] lastHash;
  }
}

void BloomFilter::setBit(unsigned int bitLocation) {
  buffer[bitLocation / 8] |= 1 << bitLocation % 8;
}

bool BloomFilter::isBitSet(unsigned int bitLocation) {
  return !!(buffer[bitLocation / 8] & 1 << bitLocation % 8);
}

void BloomFilter::add(const char *input, int len) {
  for (int j = 0; j < numHashFunctions; j++) {
    setBit(hashFunctions[j](input, len) % bitBufferSize);
  }
}

void BloomFilter::add(const char *sz) {
  add(sz, static_cast<int>(strlen(sz)));
}

bool BloomFilter::exists(const char *input, int len) {
  bool allSet = true;
  for (int j = 0; j < numHashFunctions; j++) {
    allSet = allSet && isBitSet(hashFunctions[j](input, len) % bitBufferSize);
  }
  return allSet;
}

bool BloomFilter::exists(const char *sz) {
  return exists(sz, static_cast<int>(strlen(sz)));
}

void BloomFilter::getHashesForCharCodes(const char *input, int inputLen,
    uint64_t *lastHashes, uint64_t *newHashes, unsigned char lastCharCode) {
  for (int i = 0; i < numHashFunctions; i++) {
    if (lastHashes) {
      *(newHashes + i) = hashFunctions[i](input, inputLen,
          lastCharCode, *(lastHashes+i));
    } else {
      *(newHashes + i) = hashFunctions[i](input, inputLen);
    }
  }
}

bool BloomFilter::substringExists(const char *data, int dataLen,
    int substringLength) {
  unsigned char lastCharCode = 0;
  for (int i = 0; i < dataLen - substringLength + 1; i++) {
    getHashesForCharCodes(data + i, substringLength, i == 0
        ? nullptr : lastHash, lastHash, lastCharCode);
    bool allSet = true;
    for (int j = 0; j < numHashFunctions; j++) {
      allSet = allSet && isBitSet(lastHash[j] % bitBufferSize);
    }
    if (allSet) {
      return true;
    }
    lastCharCode = data[i];
  }
  return false;
}

bool BloomFilter::substringExists(const char *data, int substringLength) {
  return substringExists(data, static_cast<int>(strlen(data)), substringLength);
}

void BloomFilter::clear() {
  memset(buffer, 0, byteBufferSize);
}
