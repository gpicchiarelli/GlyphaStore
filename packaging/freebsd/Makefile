PORTNAME=       glyphastore
DISTVERSION=    0.1.0
CATEGORIES=     databases
MASTER_SITES=   https://github.com/gpicchiarelli/GlyphaStore/releases/download/v${DISTVERSION}/
DISTNAME=       GlyphaStore-${DISTVERSION}
EXTRACT_SUFX=   .tar.xz

MAINTAINER=     gpicchiarelli@gmail.com
COMMENT=        Paired-reader/writer in-memory key-value storage prototype
WWW=            https://github.com/gpicchiarelli/GlyphaStore

LICENSE=        BSD3CLAUSE
LICENSE_FILE=   ${WRKSRC}/LICENSE

USES=           cmake:testing compiler:c++23-lang pathfix pkgconfig ssl
USE_LDCONFIG=   yes
USE_RC_SUBR=    glyphastored
USERS=          glyphastore
GROUPS=         glyphastore

CMAKE_ON=       GLYPHASTORE_WARNINGS_AS_ERRORS
CMAKE_OFF=      GLYPHASTORE_BUILD_BENCHMARKS GLYPHASTORE_BUILD_FUZZERS \
                GLYPHASTORE_FAULT_INJECTION GLYPHASTORE_NATIVE_CPU
CMAKE_ARGS=     -DGLYPHASTORE_ENABLE_TLS=ON

TEST_TARGET=    test

post-install:
	${INSTALL_DATA} ${FILESDIR}/glyphastored.conf.sample \
		${STAGEDIR}${PREFIX}/etc/glyphastored.conf.sample
	${MKDIR} ${STAGEDIR}/var/db/glyphastore

.include <bsd.port.mk>
