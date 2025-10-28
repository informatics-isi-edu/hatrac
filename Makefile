

# this ugly hack necessitated by Ubuntu... grrr...
SYSPREFIX=$(shell python3 -c 'import site;print(site.getsitepackages()[0])' | sed -e 's|/[^/]\+/[^/]\+/[^/]\+$$||')
# try to find the architecture-neutral lib dir by looking for one of our expected prereqs... double grrr...
PYLIBDIR=$(shell python3 -c 'import site;import os.path;print([d for d in site.getsitepackages() if os.path.exists(d+"/globus_sdk")][0])')

CONFDIR=/etc
SHAREDIR=$(SYSPREFIX)/share/hatrac

ifndef HATRAC_ADMIN_GROUP
override HATRAC_ADMIN_GROUP = "https://auth.globus.org/3938e0d0-ed35-11e5-8641-22000ab4b42b"
endif

ifeq ($(wildcard /etc/httpd/conf.d),/etc/httpd/conf.d)
	HTTPSVC=httpd
else
	HTTPSVC=apache2
endif

HTTPDCONFDIR=/etc/$(HTTPSVC)/conf.d
WSGISOCKETPREFIX=/var/run/$(HTTPSVC)/wsgi
DAEMONUSER=hatrac
DAEMONHOME=$(shell getent passwd $(DAEMONUSER) | cut -f6 -d: )

# turn off annoying built-ins
.SUFFIXES:

INSTALL=./install-script

# make this the default target
install: force
	pip3 install --upgrade .

testvars: force
	@echo DAEMONUSER=$(DAEMONUSER)
	@echo DAEMONHOME=$(DAEMONHOME)
	@echo CONFDIR=$(CONFDIR)
	@echo SYSPREFIX=$(SYSPREFIX)
	@echo SHAREDIR=$(SHAREDIR)
	@echo HTTPDCONFDIR=$(HTTPDCONFDIR)
	@echo WSGISOCKETPREFIX=$(WSGISOCKETPREFIX)
	@echo PYLIBDIR=$(PYLIBDIR)

wsgi_hatrac.conf: force
	su -c \
		'python3 -c "import hatrac as m;m.sample_httpd_config()"' \
		- hatrac > $@

$(HTTPDCONFDIR)/%.conf: ./%.conf force
	$(INSTALL) -o root -g root -m a+r -p -D -n $< $@

$(DAEMONHOME)/%config.json: test/%config.json force
	$(INSTALL) -o root -g apache -m a+r -p -D -n $< $@

DEPLOY_FILES=\
	$(HTTPDCONFDIR)/wsgi_hatrac.conf \
	$(DAEMONHOME)/hatrac_config.json

deploy: $(DEPLOY_FILES) force
	su -c "hatrac-deploy $(HATRAC_ADMIN_GROUP)" - hatrac

deploy-full: $(DEPLOY_FILES) force
	su -c "createdb -O hatrac hatrac" - postgres
	su -c "hatrac-deploy $(HATRAC_ADMIN_GROUP)" - hatrac
	mkdir -p /var/www/hatrac
	chown hatrac /var/www/hatrac

uninstall: force
	pip3 uninstall -y hatrac

force:

