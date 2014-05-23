#include "SugarDB.h"

/* Deleting the current record specified by the database structure.
This function is called by db_delete() and db_store() after the record has been located by _db_find() */
int _db_dodelete(DB *db)
{
	int i;
	char *ptr;
	off_t freeptr, saveptr;
	/* Setting data buffer to all blanks */
	for(ptr = db->datbuf, i = 0; i < db->datlen - 1; i++)
		*ptr++ = ' ';
	*ptr = 0; /*null terminate for _db_writedat()*/
	/* Setting key to blanks */
	ptr = db->idxbuf;
	while(*ptr)
		*ptr++ = ' ';
	/* Locking the free list */
	if(writew_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	/* Writing the data record with all blanks */
	_db_writedat(db, db->datbuf, db->datoff, SEEK_SET);
	/* Reading the free list pointer.
	It's value becomes the chain ptr field of the deleted index record.
	This means that the deleted  record becomes the head of the free list. */
	freeptr = _db_readptr(db, FREE_OFF);
	/* Save the contents of the index record chain ptr before being re-written by _db_writeidx() */
	saveptr = db->ptrval;
	/* Re-writing the index record, which also re-writes the length of the index record , data offset & data length*/
	_db_writeidx(db, db->idxbuf, db->idxoff, SEEK_SET, freeptr);
	/* Writing the new free list pointer */
	_db_writeptr(db, FREE_OFF, db->idxoff);
	/* Re-writing the chain ptr that pointed to this record being deleted.
	Recalling that _db_find() sets db->ptroff to point to this chain ptr.
	We'll be setting this chain ptr to the contents of the deleted record's chain ptr, saveptr, which can be either zero or not */
	_db_writeptr(db, db->ptroff, saveptr);
	if(un_lock(db->idxfd, FREE_OFF, SEEK_SET, 1) < 0)
		err_dump("error");
	return(0);
}
