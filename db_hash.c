#include "SugarDB.h"
/* Calculating the hash value for the key */
hash_t _db_hash(DB *db, const char *key)
{
	hash_t hval;
	const char *ptr;
	char c;
	int i;
	hval = 0;
	for(ptr = key, i = 1; c = ptr++; i++)
		hval += c * i; /* ASCII char times it's l-based index */
	return(hval % db->nhash);
}
