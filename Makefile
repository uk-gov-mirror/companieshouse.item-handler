artifact_name       := item-handler
version             := "unversioned"

.PHONY: all
all: build

.PHONY: submodules
submodules:
	git submodule init
	git submodule update

.PHONY: clean
clean:
	mvn clean
	rm -f ./$(artifact_name).jar
	rm -f ./$(artifact_name)-*.zip
	rm -rf ./build-*

.PHONY: build
build: submodules
	mvn versions:set -DnewVersion=$(version) -DgenerateBackupPoms=false
	mvn package -DskipTests=true
	cp ./target/$(artifact_name)-$(version).jar ./$(artifact_name).jar

.PHONY: docker-image
docker-image: clean
	mvn package -Dskip.unit.tests=true -Dskip.integration.tests=true jib:dockerBuild

.PHONY: test
test: test-unit test-integration

.PHONY: test-unit
test-unit: clean
	mvn test

.PHONY: test-integration
test-integration: clean
	mvn -Dtest=*IntegrationTest test

.PHONY: test-contract-provider
test-contract-provider: clean
	mvn -Dtest=*ProviderContractTest test

.PHONY: test-contract-consumer
test-contract-consumer: clean
	mvn -Dtest=*ConsumerContractTest test

.PHONY: dev
dev: clean
	mvn package -DskipTests=true
	cp target/$(artifact_name)-unversioned.jar $(artifact_name).jar

.PHONY: coverage
coverage: clean
	mvn versions:set -DnewVersion=$(version) -DgenerateBackupPoms=false jacoco:prepare-agent test \
	jacoco:prepare-agent-integration verify \
	jacoco:report jacoco:report-integration

.PHONY: package
package:
ifndef version
	$(error No version given. Aborting)
endif
	$(info Packaging version: $(version))
	mvn versions:set -DnewVersion=$(version) -DgenerateBackupPoms=false
	mvn package -DskipTests=true
	$(eval tmpdir:=$(shell mktemp -d build-XXXXXXXXXX))
	cp ./target/$(artifact_name)-$(version).jar $(tmpdir)/$(artifact_name).jar
	cp -r ./api-enumerations $(tmpdir)
	cd $(tmpdir); zip -r ../$(artifact_name)-$(version).zip *
	rm -rf $(tmpdir)

.PHONY: dist
dist: clean build package
