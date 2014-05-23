#include "SugarDB.h"

/* Storing a record in a database.
Return 0 if okay, Return 1 if record exists and DB_INSERT specified.
Return -1 if record doesn't exist and DB_REPLACE specified.
*/
int db_store(DB *db, const char *key, const char *data, int flag)
{
	int rc, keylen, datlen;
	off_t ptrval;
	keylen = strlen(key);
	datlen = strlen(data) + 1; /* +1 is for newline at the end */
	if(datlen < DATLEN_MIN || datalen > DATLEN_MAX)
		err_dump("invalid data length");
	/* db_find() calculates which hash table this new record goes into(db->chainoff) whether it already exists or not.
	The calls to _db_writeptr() below change the hash table entry for this chain to point to new record.
	This means that the new record is added to the front of the hash chain */
	if(_db_find(db, key, 1) < 0)
	{
		/* record not found*/
		if(flag & DB_REPLACE)
		{
			rc = -1;
			db->cnt_storerr++;
			goto doreturn; /* error --- record doesn't exist */
		}
		/* do_find() locked the hash chain.
			Reading the chain ptr to the first index record on hash chain */
		ptrval = _db_readptr(db, db->chainoff);
		if(_db_findfree(db, keylen, datlen) < 0)
		{
			/* an empty record of the correct record wasn't found.
				we gotta append the new record to the ends of the index and data files */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* db->idxoff was set by _db_writeidx()
			The record goes to the front of the bash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor1++;
		}
		else
		{
			/* Re-using an empty record
			_db_findtree() removed the record from the free list and set both db->datoff and db->idxoff */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* re-used record goes to the front of the hash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor2++;
		}
	}
	else
	{
		/* record found */
		if(flag & DB_INSERT)
		{
			rc = 1;
			db->cnt_storerr++;
			goto doreturn; /* error --- record is already in the database */
		}
		/* We are replacing the existing record.
			We know the new key equals the existing key, but also we gotta check if the data records are the same size. */
		if(datlen != db->datlen)
		{
			_db_dodelete(db); /* delete the existing record */
			/* Re-read the chain ptr in the hash table (It may change with the deletion) */
			ptrval = _db_readptr(db, db->chainoff);
			/* Appending new index and data records to end of files */
			_db_writedat(db, data, 0, SEEK_END);
			_db_writeidx(db, key, 0, SEEK_END, ptrval);
			/* new record going to the front of the hash chain */
			_db_writeptr(db, db->chainoff, db->idxoff);
			db->cnt_stor3++;
		}
		else
		{
			/* Same sized data, just replaces data record */
			_db_writedat(db, data, db->datoff, SEEK_SET);
			db->cnt_stor4++;
		}
	}
	rc = 0; /* woo hoo */
doreturn: /* unlock the hash chain that _db_find() locked */
	if(un_lock(db->idxfd, db->chainoff, SEEK_SET, 1) < 0)
		return(rc);
}
