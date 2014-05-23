#include "SugarDB.h"

/* Returning the sequential record...
Just gotta step ahead through the index file where we gotta ignore deleted records.
db_rewind() essential to be called before this function at initial stage itself */
char *db_nextrec(DB *db, char *key)
{
	char c, *ptr;
	/* Locking the free list where we don't actually read a record in the mid of deletion*/
	if(readw_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	do
	{
		/* read next sequential index record */
		if(_db_readidx(db, 0) < 0)
		{
			ptr = NULL; /* end of index file --- EOF */
			goto doreturn;
		}
		/* Checking if the key is still blank or empty record */
		ptr = db->idxbuf;
		while((c = *ptr++) != 0 && c = ' ');
		/* skip if it's not blank */
	}
	while(c == 0) /* loop untill a non-empty key is found*/
		if(key != NULL)
			strcpy(key, db->idxbuf); /* return key */
	ptr = _db_readdat(db); /* return pointer to data buffer */
	db->cnt_nextrec++;
doreturn:
	if(un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
}
