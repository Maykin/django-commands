# Inspired by http://djangosnippets.org/snippets/823/
import time, os
import tempfile
from optparse import make_option

from django.core.management.base import CommandError, LabelCommand

from django_commands.utils import get_db_conf, build_mysql_args, build_postgres_args

class Command(LabelCommand):
    args = '<filename>'
    help = ("Creates a backup dump of the database. The <filename> "
            "argument \nis used for the actual backup file, "
            "but a timestamp and appropriate \nfile extension will be "
            "appended, e.g. <filename>-2000-12-31-2359.sqlite.gz.\n"
            "BEWARE OF SHELL INJECTION in database settings.")
    option_list = LabelCommand.option_list + (
        make_option('--database', action='store', dest='database',
            help='Target database. Defaults to the "default" database.'),
    )

    def handle_label(self, label, **options):
        db_conf = get_db_conf(options)

        backup_handler = getattr(self, '_backup_%s_db' % db_conf['engine'])

        try:
            tmp_outfile = tempfile.NamedTemporaryFile(mode='w')

            ret, outfile = backup_handler(db_conf, "%s-%s" %
                    (label, time.strftime('%Y-%m-%d-%H%M')),
                    tmp_outfile)
            if ret:
                raise IOError()

            _check_writable(outfile)

            ret = os.system('gzip -c %s -9 > %s' % (tmp_outfile.name, outfile))
            if ret:
                raise IOError()
        except IOError:
            # Cleanup empty output file if something went wrong
            os.system('rm %s' % outfile)
            raise CommandError("Database '%s' backup to '%s' failed" %
                    (db_conf['db_name'], outfile))
        finally:
            tmp_outfile.close()

        print ("Database '%s' successfully backed up to: %s" %
                (db_conf['db_name'], outfile))

    def _backup_sqlite3_db(self, db_conf, outfile, tmp_outfile):
        outfile = '%s.sqlite.gz' % outfile
        ret = os.system('sqlite3 %s .dump > %s' %
                (db_conf['db_name'], tmp_outfile.name))

        return ret, outfile

    def _backup_postgresql_db(self, db_conf, outfile):
        return self._backup_postgresql_psycopg2_db(db_conf, outfile)

    def _backup_postgresql_psycopg2_db(self, db_conf, outfile, tmp_outfile):
        passwd = ('export PGPASSWORD=%s;' % db_conf['password']
                    if db_conf['password'] else '')
        outfile = '%s.pgsql.gz' % outfile
        ret = os.system('%s pg_dump %s > %s' %
                (passwd, build_postgres_args(db_conf), tmp_outfile.name))

        return ret, outfile

    def _backup_mysql_db(self, db_conf, outfile, tmp_outfile):
        outfile = '%s.mysql.gz' % outfile
        ret = os.system('mysqldump %s > %s' %
                (build_mysql_args(db_conf), tmp_outfile.name))

        return ret, outfile

# Be aware of the classic race condition here.
def _check_writable(filename):
    if os.path.exists(filename):
        raise CommandError("'%s' already exists, won't overwrite." % filename)
    dir_path = os.path.dirname(filename)
    if not os.access(dir_path, os.W_OK):
        raise CommandError("Directory '%s' is not writable." % dir_path)

    # or, more stringent:
    # from __future__ import with_statement
    # try:
        # with open(filename, 'w'): pass
    # except Exception, e:
        # raise CommandError("Cannot open '%s' for writing: %s %s" %
                # (filename, e.__class__.__name__, e))
