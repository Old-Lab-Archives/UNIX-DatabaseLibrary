#include "SugarDB.h"

/* Rewinding the index file for db_nextrec()
Then, automatically called by db_open()
And it must be called before db_nextrec() --- oops! */
void db_rewind(DB *db)
{
	off_t offset;
	offset = (db->nhash + 1) * PTR_SZ; /* +1 for free list pointer */
	/* Then, we are just setting the file offset for this process to the start of the index records,
		Also, not required to lock.
		+1 for newline at the end of hash table
	*/
	if((db->idxoff = lseek(db->idxfd, offset+1, SEEK_SET)) == -1)
		err_dump("error");
}

