V =             0.1.0
COMMENT =       paired-reader/writer key-value storage prototype
DISTNAME =      GlyphaStore-${V}
PKGNAME =       glyphastore-${V}
CATEGORIES =    databases

HOMEPAGE =      https://github.com/gpicchiarelli/GlyphaStore
MASTER_SITES =  https://github.com/gpicchiarelli/GlyphaStore/releases/download/v${V}/
EXTRACT_SUFX =  .tar.xz

MAINTAINER =    Giacomo Picchiarelli <gpicchiarelli@gmail.com>
PERMIT_PACKAGE = Yes

# ABI_VERSION authority: validated against ../../ABI_VERSION by
# engineering/tools/validate_bsd_packaging.py.
SHARED_LIBS +=  glyphastore 1.0

WANTLIB +=      c c++ c++abi crypto m pthread ssl

MODULES =       devel/cmake
BUILD_DEPENDS = devel/ninja
CONFIGURE_ARGS += -DGLYPHASTORE_ENABLE_TLS=ON \
                  -DGLYPHASTORE_BUILD_BENCHMARKS=OFF \
                  -DGLYPHASTORE_BUILD_FUZZERS=OFF \
                  -DGLYPHASTORE_FAULT_INJECTION=OFF \
                  -DGLYPHASTORE_NATIVE_CPU=OFF \
                  -DGLYPHASTORE_WARNINGS_AS_ERRORS=ON \
                  -DCMAKE_INSTALL_MANDIR=man

post-install:
	${INSTALL_DATA_DIR} ${PREFIX}/share/examples/glyphastore
	${INSTALL_DATA} ${FILESDIR}/glyphastored.conf \
		${PREFIX}/share/examples/glyphastore/glyphastored.conf

.include <bsd.port.mk>
