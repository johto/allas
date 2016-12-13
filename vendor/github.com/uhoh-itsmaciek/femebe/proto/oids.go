package proto

// TypSize returns the size in bytes of the Postgres type specified by
// typOid, where undertood by femebe. For variable-length types or if
// the type is not known, -1 is returned.
func TypSize(typOid Oid) int16 {
	// TODO: right now, we hardcode the length of the various types
	// here; ideally, we should have a mapping for the fixed-length
	// types (although it seems that all the dynamic-length types,
	// even those with explicit length restrictions, is just -1, so
	// perhaps we can provide a mapping for that as well).
	switch typOid {
	case OidBool:
		return 1
	case OidInt2:
		return 2
	case OidInt4, OidFloat4:
		return 4
	case OidInt8, OidFloat8:
		return 8
	case OidText:
		return -1
	default:
		// unknown, assume variable length
		return -1
	}
}

type Oid uint32

// The oids of the Postgres built-in types
const (
	// generated via
	// psql -qAt -F $'\t' -p 5434 postgres -c
	//   "select 'OID_' || upper(typname) || '=' || oid from pg_type"
	// and pared down
	OidBool            Oid = 16
	OidBytea               = 17
	OidChar                = 18
	OidName                = 19
	OidInt8                = 20
	OidInt2                = 21
	OidInt2vector          = 22
	OidInt4                = 23
	OidRegproc             = 24
	OidText                = 25
	OidOid                 = 26
	OidTid                 = 27
	OidXid                 = 28
	OidCid                 = 29
	OidOidvector           = 30
	OidPgType              = 71
	OidPgAttribute         = 75
	OidPgProc              = 81
	OidPgClass             = 83
	OidJson                = 114
	OidXml                 = 142
	OidSmgr                = 210
	OidPoint               = 600
	OidLseg                = 601
	OidPath                = 602
	OidBox                 = 603
	OidPolygon             = 604
	OidLine                = 628
	OidFloat4              = 700
	OidFloat8              = 701
	OidAbstime             = 702
	OidReltime             = 703
	OidTinterval           = 704
	OidUnknown             = 705
	OidCircle              = 718
	OidMoney               = 790
	OidMacaddr             = 829
	OidInet                = 869
	OidCidr                = 650
	OidAclitem             = 1033
	OidBpchar              = 1042
	OidVarchar             = 1043
	OidDate                = 1082
	OidTime                = 1083
	OidTimestamp           = 1114
	OidTimestamptz         = 1184
	OidInterval            = 1186
	OidTimetz              = 1266
	OidBit                 = 1560
	OidVarbit              = 1562
	OidNumeric             = 1700
	OidRefcursor           = 1790
	OidRegprocedure        = 2202
	OidRegoper             = 2203
	OidRegoperator         = 2204
	OidRegclass            = 2205
	OidRegtype             = 2206
	OidUuid                = 2950
	OidTsvector            = 3614
	OidGtsvector           = 3642
	OidTsquery             = 3615
	OidRegconfig           = 3734
	OidRegdictionary       = 3769
	OidTxidSnapshot        = 2970
	OidInt4range           = 3904
	OidNumrange            = 3906
	OidTsrange             = 3908
	OidTstzrange           = 3910
	OidDaterange           = 3912
	OidInt8range           = 3926
	OidRecord              = 2249
	OidCstring             = 2275
	OidAny                 = 2276
	OidAnyarray            = 2277
	OidVoid                = 2278
	OidTrigger             = 2279
	OidLanguageHandler     = 2280
	OidInternal            = 2281
	OidOpaque              = 2282
	OidAnyelement          = 2283
	OidAnynonarray         = 2776
	OidAnyenum             = 3500
	OidFdwHandler          = 3115
	OidAnyrange            = 3831
)
