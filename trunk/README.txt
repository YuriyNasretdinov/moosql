INTRO:

    MooSQL is a project intended to substitute MySQL in PHP if you don't have MySQL.
    Currently there is no SQL support, so actually you may not want to use it now.
    However, if you still do, consider reading HOWTO:

    http://code.google.com/p/moosql/wiki/HOWTO

READING OR MODIFYING SOURCE:

    If you are to read the source, please be sure, that:

     - You really want it.
     - You understand why you need this.
     - You at least realize how a simple RDBMS works.
     - You agree to refrain from criticism of the code that you see until you have
       read it very carefully and understood the code style and conventions that
       are used at least in "core/" section. Be sure that this code works and it
       works well. This could not be achieved if the approaches used were completely
       wrong. So, you were warned.
    
    So, if you are still reading this, here is a brief explanation of a file
    structure:
    
     ./Client.php             -- the front-end file for MooSQL that is the only that
                                 should be included if you want to use the library.
     ./DataInterface.php      -- the simple class to get low-level access to
                                 database data and structure.
    
     ./core/                  -- the directory with the storage engine classes, API
                                 of YNDb being documented at HOWTO page in Project
                                 Wiki (see INTRO).
                                 All files in ./core/ have prefix "YN", e.g. in
                                 Db.php you will see definition of YNDb class.
     ./core/Db.php            -- the definition of YNDb class, the relatively
                                 high-level interface to the storage engine. Most
                                 of logic that works with table data (without
                                 indexes) is contained in this file.
     ./core/Index.php         -- the definition of YNIndex class, which provides a
                                 rather high-level interface to work with column
                                 indexes.
                                 This class makes extensive use of YNBTree_gen and
                                 YNBTree_Idx_gen classes.
     ./core/BTree_gen.php     -- The generalized (meaning that it can contain mostly
                                 any fixed-width type) B-Tree implementation in PHP.
                                 BTree is used for UNIQUE indexes
     ./core/BTree_Idx_gen.php -- The BTree+List index, used for INDEX index :).
     ./core/fopen-cacher.php  -- The collection of functions to provide descriptor
                                 caching functionality in PHP.
     