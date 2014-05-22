#include "SugarDB.h"
/* Find specified record.
Called by db_delete(), db_fetch() and db_store() */
int _db_find(DB *db, const char *key, int writelock)
{
	off_t offset, nextoffset;
	/* calculate hash value for this key, then calculate byte offset of corresponding chain ptr in hash table.
	*/
	/* calculate offset in hash table for this key */
	db->chainoff = (_db_hash(db, key) * PTR_SZ) + db->hashoff;
	db->ptroff = db->chainoff;
	/* here's where we lock this hash chain. It's the caller responsibility to unlock it when done.
	Note that we lock and unlock only the first byte. */
	if(writelock)
	{
		if(writew_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("error");
	}
	else
	{
		if(readw_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
			err_dump("error");
	}
	/* Get the offset in the index file of first record on the bash chain (it can be 0 too) */
	offset = _db_readptr(db, db->ptroff);
	while(offset!=0)
	{
		nextoffset = _db_readidx(db, offset);
		if(strcmp(db->idxbuf, key) == 0)
			break; /* found a match */
		db->ptroff = offset; /* offset of this (unequal) record */
		offset = nextoffset; /* next one to compare */
	}
	if(offset == 0)
		return(-1); /* error -- record not found */
	return(0); /* if not error */
}
