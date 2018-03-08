Name:           pg_back
Version:        1.5
Release:        1
License:        BSD
Summary:        Simple backup script for PostgreSQL
Group:          Applications/Databases
Source0:        https://github.com/orgrim/pg_back/archive/v%{version}.tar.gz
BuildArch:      noarch
Requires:       bash >= 3.2

%description
pg_back uses pg_dumpall to dump roles and tablespaces, pg_dump to dump
each selected database to a separate file. The custom format of
pg_dump is used by default.

A configuration file, by default /etc/postgresql/pg_back.conf, can
hold the configuration to automate the backup. All options can be
overridden on the command line.

Databases to dump can be specified in the configuration file or on the
command line.  A list fo databases can also be excluded. Database
templates can be included, with the exception of template0, because
connection to it are forbidden by default.

The purpose of the script is to allow unattended backups, thus a purge
time can be configured to avoid running out of disk space in the
backup directory. It is set to 30 days by default.

The script is working out of the box, but you should consider editing
it to fit your needs. This is why I want to keep it the simplest
possible.

%prep
%setup -q

%build

%install
install -D -p -m 0755 pg_back %{buildroot}/usr/bin/pg_back
install -D -p -m 0644 pg_back.conf %{buildroot}/etc/postgresql/pg_back.conf
install -D -p -m 0644 CHANGELOG %{buildroot}/usr/share/doc/pg_back-%{version}/CHANGELOG
install -D -p -m 0644 CHANGELOG %{buildroot}/usr/share/doc/pg_back-%{version}/CONTRIBUTORS
install -D -p -m 0644 CHANGELOG %{buildroot}/usr/share/doc/pg_back-%{version}/README

%clean

%files
%defattr(-,root,root)
%doc /usr/share/doc/pg_back-%{version}/CHANGELOG
%doc /usr/share/doc/pg_back-%{version}/CONTRIBUTORS
%doc /usr/share/doc/pg_back-%{version}/README
%config(noreplace) /etc/postgresql/%{name}.conf
%{_bindir}/pg_back

%changelog
* Thu Mar  8 2018 Nicolas Thauvin <nico@orgrim.net> - 1.5-1
* New upstream release

* Thu Jan 18 2018 Étienne BERSAC <etienne.bersac@dalibo.com> - 1.4-1
- Initial packaging
