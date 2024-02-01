// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        v3.21.12
// source: nondeterministic.proto

package pb

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type RowInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DiffLineIdx int64   `protobuf:"varint,1,opt,name=diff_line_idx,json=diffLineIdx,proto3" json:"diff_line_idx,omitempty"`
	ColNumber   []int64 `protobuf:"varint,2,rep,packed,name=col_number,json=colNumber,proto3" json:"col_number,omitempty"`
}

func (x *RowInfo) Reset() {
	*x = RowInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nondeterministic_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RowInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RowInfo) ProtoMessage() {}

func (x *RowInfo) ProtoReflect() protoreflect.Message {
	mi := &file_nondeterministic_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RowInfo.ProtoReflect.Descriptor instead.
func (*RowInfo) Descriptor() ([]byte, []int) {
	return file_nondeterministic_proto_rawDescGZIP(), []int{0}
}

func (x *RowInfo) GetDiffLineIdx() int64 {
	if x != nil {
		return x.DiffLineIdx
	}
	return 0
}

func (x *RowInfo) GetColNumber() []int64 {
	if x != nil {
		return x.ColNumber
	}
	return nil
}

type DiffInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FromLineNumber int64      `protobuf:"varint,1,opt,name=from_line_number,json=fromLineNumber,proto3" json:"from_line_number,omitempty"`
	FromLineCnt    int64      `protobuf:"varint,2,opt,name=from_line_cnt,json=fromLineCnt,proto3" json:"from_line_cnt,omitempty"`
	ToLineNumber   int64      `protobuf:"varint,3,opt,name=to_line_number,json=toLineNumber,proto3" json:"to_line_number,omitempty"`
	ToLineCnt      int64      `protobuf:"varint,4,opt,name=to_line_cnt,json=toLineCnt,proto3" json:"to_line_cnt,omitempty"`
	RowInfo        []*RowInfo `protobuf:"bytes,5,rep,name=row_info,json=rowInfo,proto3" json:"row_info,omitempty"`
}

func (x *DiffInfo) Reset() {
	*x = DiffInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nondeterministic_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DiffInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DiffInfo) ProtoMessage() {}

func (x *DiffInfo) ProtoReflect() protoreflect.Message {
	mi := &file_nondeterministic_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DiffInfo.ProtoReflect.Descriptor instead.
func (*DiffInfo) Descriptor() ([]byte, []int) {
	return file_nondeterministic_proto_rawDescGZIP(), []int{1}
}

func (x *DiffInfo) GetFromLineNumber() int64 {
	if x != nil {
		return x.FromLineNumber
	}
	return 0
}

func (x *DiffInfo) GetFromLineCnt() int64 {
	if x != nil {
		return x.FromLineCnt
	}
	return 0
}

func (x *DiffInfo) GetToLineNumber() int64 {
	if x != nil {
		return x.ToLineNumber
	}
	return 0
}

func (x *DiffInfo) GetToLineCnt() int64 {
	if x != nil {
		return x.ToLineCnt
	}
	return 0
}

func (x *DiffInfo) GetRowInfo() []*RowInfo {
	if x != nil {
		return x.RowInfo
	}
	return nil
}

type DiffInfos struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	DiffInfo []*DiffInfo `protobuf:"bytes,1,rep,name=diff_info,json=diffInfo,proto3" json:"diff_info,omitempty"`
}

func (x *DiffInfos) Reset() {
	*x = DiffInfos{}
	if protoimpl.UnsafeEnabled {
		mi := &file_nondeterministic_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *DiffInfos) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DiffInfos) ProtoMessage() {}

func (x *DiffInfos) ProtoReflect() protoreflect.Message {
	mi := &file_nondeterministic_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DiffInfos.ProtoReflect.Descriptor instead.
func (*DiffInfos) Descriptor() ([]byte, []int) {
	return file_nondeterministic_proto_rawDescGZIP(), []int{2}
}

func (x *DiffInfos) GetDiffInfo() []*DiffInfo {
	if x != nil {
		return x.DiffInfo
	}
	return nil
}

var File_nondeterministic_proto protoreflect.FileDescriptor

var file_nondeterministic_proto_rawDesc = []byte{
	0x0a, 0x16, 0x6e, 0x6f, 0x6e, 0x64, 0x65, 0x74, 0x65, 0x72, 0x6d, 0x69, 0x6e, 0x69, 0x73, 0x74,
	0x69, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x6d, 0x61, 0x69, 0x6e, 0x22, 0x4c,
	0x0a, 0x07, 0x52, 0x6f, 0x77, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x22, 0x0a, 0x0d, 0x64, 0x69, 0x66,
	0x66, 0x5f, 0x6c, 0x69, 0x6e, 0x65, 0x5f, 0x69, 0x64, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03,
	0x52, 0x0b, 0x64, 0x69, 0x66, 0x66, 0x4c, 0x69, 0x6e, 0x65, 0x49, 0x64, 0x78, 0x12, 0x1d, 0x0a,
	0x0a, 0x63, 0x6f, 0x6c, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x03, 0x52, 0x09, 0x63, 0x6f, 0x6c, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x22, 0xc8, 0x01, 0x0a,
	0x08, 0x44, 0x69, 0x66, 0x66, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x28, 0x0a, 0x10, 0x66, 0x72, 0x6f,
	0x6d, 0x5f, 0x6c, 0x69, 0x6e, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x03, 0x52, 0x0e, 0x66, 0x72, 0x6f, 0x6d, 0x4c, 0x69, 0x6e, 0x65, 0x4e, 0x75, 0x6d,
	0x62, 0x65, 0x72, 0x12, 0x22, 0x0a, 0x0d, 0x66, 0x72, 0x6f, 0x6d, 0x5f, 0x6c, 0x69, 0x6e, 0x65,
	0x5f, 0x63, 0x6e, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x0b, 0x66, 0x72, 0x6f, 0x6d,
	0x4c, 0x69, 0x6e, 0x65, 0x43, 0x6e, 0x74, 0x12, 0x24, 0x0a, 0x0e, 0x74, 0x6f, 0x5f, 0x6c, 0x69,
	0x6e, 0x65, 0x5f, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x52,
	0x0c, 0x74, 0x6f, 0x4c, 0x69, 0x6e, 0x65, 0x4e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x12, 0x1e, 0x0a,
	0x0b, 0x74, 0x6f, 0x5f, 0x6c, 0x69, 0x6e, 0x65, 0x5f, 0x63, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x03, 0x52, 0x09, 0x74, 0x6f, 0x4c, 0x69, 0x6e, 0x65, 0x43, 0x6e, 0x74, 0x12, 0x28, 0x0a,
	0x08, 0x72, 0x6f, 0x77, 0x5f, 0x69, 0x6e, 0x66, 0x6f, 0x18, 0x05, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x0d, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x52, 0x6f, 0x77, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x07,
	0x72, 0x6f, 0x77, 0x49, 0x6e, 0x66, 0x6f, 0x22, 0x38, 0x0a, 0x09, 0x44, 0x69, 0x66, 0x66, 0x49,
	0x6e, 0x66, 0x6f, 0x73, 0x12, 0x2b, 0x0a, 0x09, 0x64, 0x69, 0x66, 0x66, 0x5f, 0x69, 0x6e, 0x66,
	0x6f, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6d, 0x61, 0x69, 0x6e, 0x2e, 0x44,
	0x69, 0x66, 0x66, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x08, 0x64, 0x69, 0x66, 0x66, 0x49, 0x6e, 0x66,
	0x6f, 0x42, 0x05, 0x5a, 0x03, 0x70, 0x62, 0x2f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_nondeterministic_proto_rawDescOnce sync.Once
	file_nondeterministic_proto_rawDescData = file_nondeterministic_proto_rawDesc
)

func file_nondeterministic_proto_rawDescGZIP() []byte {
	file_nondeterministic_proto_rawDescOnce.Do(func() {
		file_nondeterministic_proto_rawDescData = protoimpl.X.CompressGZIP(file_nondeterministic_proto_rawDescData)
	})
	return file_nondeterministic_proto_rawDescData
}

var file_nondeterministic_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_nondeterministic_proto_goTypes = []interface{}{
	(*RowInfo)(nil),   // 0: main.RowInfo
	(*DiffInfo)(nil),  // 1: main.DiffInfo
	(*DiffInfos)(nil), // 2: main.DiffInfos
}
var file_nondeterministic_proto_depIdxs = []int32{
	0, // 0: main.DiffInfo.row_info:type_name -> main.RowInfo
	1, // 1: main.DiffInfos.diff_info:type_name -> main.DiffInfo
	2, // [2:2] is the sub-list for method output_type
	2, // [2:2] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_nondeterministic_proto_init() }
func file_nondeterministic_proto_init() {
	if File_nondeterministic_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_nondeterministic_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RowInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_nondeterministic_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DiffInfo); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_nondeterministic_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*DiffInfos); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_nondeterministic_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_nondeterministic_proto_goTypes,
		DependencyIndexes: file_nondeterministic_proto_depIdxs,
		MessageInfos:      file_nondeterministic_proto_msgTypes,
	}.Build()
	File_nondeterministic_proto = out.File
	file_nondeterministic_proto_rawDesc = nil
	file_nondeterministic_proto_goTypes = nil
	file_nondeterministic_proto_depIdxs = nil
}
