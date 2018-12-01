#include <iostream>
#include <cassert>
#include <leveldb/db.h>
#include <sys/time.h>
#include <unistd.h>
#include <iomanip>
#include <algorithm>
#include <vector>
#include <sstream>
#include <fstream>
#include <string>

struct result {
  float yseql;
  float sseqq;
  float yranl;
  float rseqq;
  float rranq;
  float size;
} ;

//address of device
std::string devname = "sdb1";
//address of mount point
std::string mountp = "/mnt/g";
//address of database
std::string dir = "/mnt/g/tmp/badger";

void remount(){
	std::string umount = "umount " + mountp;
	std::string mount = "mount /dev/" + devname +  " " + mountp;
	system(umount.c_str());
	system(mount.c_str());
}

//Test either Insert or lookup
float TestInsert(int num, int size, bool ran, bool insert){
	leveldb::DB* db;
	leveldb::Options options;
  options.compression = leveldb::kNoCompression;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, dir, &db);
	assert(status.ok());
  struct timeval start, end;
  long seconds, useconds;
	float mtime;
	std::vector<int> ranvector;
	//create a random vector to get random num without duplication
	for (int i=0; i<num; ++i)
		ranvector.push_back(i);
  std::random_shuffle ( ranvector.begin(), ranvector.end() );
	std::vector<int>::iterator it=ranvector.begin();

	gettimeofday(&start, NULL);

	for(int i = 0; i < num;  i++ ){
		std::ostringstream ssv, ssk;
		int keynum;
		if(ran){
			keynum = *it;
			it ++;
		}
		else
			keynum =  i;

		ssv << std::setw(size) << keynum;
		const std::string value = ssv.str();
		ssk << std::setw(16) << keynum;
		const std::string key = ssk.str();
		std::string valuetmp;
		leveldb::Status s;
		if(insert)
			s = db->Put(leveldb::WriteOptions(), key, value);
		else
		  s = db->Get(leveldb::ReadOptions(), key, &valuetmp);

		if (!s.ok()){
			std::cerr << s.ToString() << std::endl;
			std::cerr << i<< std::endl;;
			break;
		}

	}
	gettimeofday(&end, NULL);
	seconds  = end.tv_sec  - start.tv_sec;
  useconds = end.tv_usec - start.tv_usec;
  mtime = seconds + useconds/1000000.0;

	//std::cout << mtime  << std::endl;
	delete db;
	return  mtime;
}

//Test either Insert or lookup
float TestInteration(int num){
	leveldb::DB* db;
	leveldb::Options options;
  options.compression = leveldb::kNoCompression;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, dir, &db);
	assert(status.ok());
  struct timeval start, end;
  long seconds, useconds;
	float mtime;

	gettimeofday(&start, NULL);

	leveldb::Iterator* it = db->NewIterator(leveldb::ReadOptions());
	int i  =  0;
	for (it->SeekToFirst(); i < num; it->Next()) {
  	i++;
	}
	assert(it->status().ok());  // Check for any errors found during the scan
	delete it;

	gettimeofday(&end, NULL);
	seconds  = end.tv_sec  - start.tv_sec;
  useconds = end.tv_usec - start.tv_usec;
  mtime = (seconds+ useconds/1000000.0) + 0.5;

	//std::cout << mtime  << std::endl;
	delete db;
	return  mtime;
}

int getsize(){
	leveldb::DB* db;
	leveldb::Options options;
  options.compression = leveldb::kNoCompression;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, dir, &db);
	assert(status.ok());

	const leveldb::Slice property = "leveldb.stats";
	std::string value;
	bool success = db->GetProperty(property, &value);
	if(!success){
		std::cerr << "not success" << std::endl;
		return 0;
	}
	int size = 0;

	//where is the start line of size
	int lineesc = 3;
	std::istringstream stream1(value);
	while (!stream1.eof())
	{
		//where is the start column of size
		int blankesc = 2;
		std::string s1;
		//split by line first
		getline(stream1, s1,'\n');
		if(lineesc > 0){
			lineesc--;
			continue;
		}
		std::istringstream stream2(s1);
		while (!stream2.eof())
		{
			std::string s2;
			//split by blank then
			getline(stream2, s2,' ');
			if (s2[0]){
				if(blankesc > 0){
					blankesc--;
					continue;
				}
				else{
					size += std::stoi (s2);
					break;
				}
			}
		}
	}
	delete db;
  return size;

}

int main()
{
	int bytes[7]  = {64, 256, 1024, 4*1024, 16*1024, 64*1024, 256*1024};
	//int bytes[1]  = {256*1024};
	int lengthb = sizeof(bytes)/sizeof(bytes[0]);
	std::ofstream myfile;
  result * results = new result[lengthb];
  myfile.open ("load2.txt");
	myfile << "# x yseql sseqq yranl rseqq rranq size\n";
	std::string remove = "rm -rf " + dir;
	for(int i=0; i<lengthb; i++){
		//number of key-value pairs given the size of value
		int num = 1024*1024*1024/ (bytes[i] + 16);
		//seq load
		results[i].yseql = 1024/TestInsert(num, bytes[i], false, true);
		remount();
		//seq query
		results[i].sseqq = 256/TestInteration(num/4);
		//delete is not enough because it just marks the file
		system(remove.c_str());
		//ran load
		results[i].yranl = 1024/TestInsert(num, bytes[i], true, true);
		//seq query
		remount();
		results[i].rseqq = 256/TestInteration(num/4);
		//results[i].rseqq =TestInsert(num/4, bytes[i], false, false);
		//ran query
		remount();
		results[i].rranq =TestInsert(num/4, bytes[i], true, false);
		results[i].size = (float)getsize();
		//delete is not enough because it just marks the file
		system(remove.c_str());
	}

	for(int i=0; i<lengthb; i++){
		myfile<< i+1<<" "<<results[i].yseql<<" "<<results[i].sseqq<<" "<<results[i].yranl
		<<" "<<results[i].rseqq<<" "<<results[i].rranq<<" "<<results[i].size<<"\n";
	}

	myfile.close();
}
