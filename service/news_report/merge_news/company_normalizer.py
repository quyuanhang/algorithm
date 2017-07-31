#!/usr/bin/env python
#coding=utf8
'''
@author: cuiyan
'''
import sys,re
from segment.chinese_segmenter import ChineseSegmenter

class CompanyNormalizer:
    def __init__(self, comlib_id_name = None, comext_term_id = None, seg_dict = None):
        self.segmenter = None
        self.filter_list = {}
        if seg_dict:
            # use S2D|U2L|T2S in segmenter
            self.segmenter = ChineseSegmenter(seg_dict)
            # filter out tags from company
            file_filterlist = open('%s/CustomWord/known_tags.gbk' % seg_dict)
            for line in file_filterlist:
                line = line.strip()
                if len(line) == 0:
                    continue
                line = line.decode('gbk').encode('utf8')
                self.filter_list[line] = 0
            file_filterlist.close()
        # build all name-id mapping
        self.name2id = {}
        self.id2names = {}
        # level 1: extended term-id dict with human label
        if comext_term_id:
            file_comext = open(comext_term_id)
            for line in file_comext:
                tokens = line.strip().split('\t')
                if len(tokens) < 2: continue
                if not tokens[1].isdigit(): continue
                cid = int(tokens[1])
                term = tokens[0]
                if not self.name2id.has_key(term):
                    self.name2id[term] = cid
                if not self.id2names.has_key(cid):
                    self.id2names[cid] = set()
                self.id2names[cid].add(term)
            file_comext.close()
        if comlib_id_name:
            # level 2: original and normalized company
            file_comlib = open(comlib_id_name)
            for line in file_comlib:
                tokens = line.strip().split('\t')
                if len(tokens) < 2: continue
                if not tokens[0].isdigit(): continue
                cid = int(tokens[0])
                cname = self.com_format(tokens[1])
                if not self.name2id.has_key(cname):
                    self.name2id[cname] = cid
                if not self.id2names.has_key(cid):
                    self.id2names[cid] = set()
                self.id2names[cid].add(cname)
                cname_norms = self.com_norm(cname, batch_result=True)
                for cname_norm in cname_norms:
                    if len(cname_norm) > 0 and cname != cname_norm:
                        if not self.name2id.has_key(cname_norm):
                            self.name2id[cname_norm] = cid
                        if not self.id2names.has_key(cid):
                            self.id2names[cid] = set()
                        self.id2names[cid].add(cname_norm)
            file_comlib.close()
            # level 3: pieces of company with no high confidence
            file_comlib = open(comlib_id_name)
            for line in file_comlib:
                tokens = line.strip().split('\t')
                if len(tokens) < 2: continue
                if not tokens[0].isdigit(): continue
                cid = int(tokens[0])
                cname = self.com_format(tokens[1])
                for ext_cname in self.extract_extensions(cname):
                    if not self.name2id.has_key(ext_cname):
                        self.name2id[ext_cname] = cid
                    if not self.id2names.has_key(cid):
                        self.id2names[cid] = set()
                    self.id2names[cid].add(ext_cname)
            file_comlib.close()

    def com_format(self, com):
        format_com = com
        if self.segmenter:
            format_com = self.segmenter.normalize(com)
        else:
            format_com = com.lower().strip()
        tokens = format_com.split(' ')
        format_com = ''
        for token in tokens:
            if format_com[-1:].isalnum() and token[:1].isalnum():
                format_com += ' '
            format_com += token
        return format_com

    def com_norm(self, com, batch_result=False):
        norm_coms = []
        norm_com = self.com_format(com)
        pos = 0
        for match in re.finditer('\s*(-|\/|\()+\s*', norm_com, pos):
            if match.start() != 0 and match.end() != len(norm_com) and not (norm_com[match.start()-1].isalpha() and (match.end() == len(norm_com) or norm_com[match.end()].isalpha()) and len(match.group()) == 1):
                norm_com = norm_com[:match.start()]
                norm_coms.append(norm_com)
                break
            pos = match.end()
        for prefix in ['北京','上海','广州','深圳','杭州','成都','西安']:
            if norm_com.startswith(prefix):
                todo_norm_com = norm_com[len(prefix):]
                if todo_norm_com.startswith('市'):
                    todo_norm_com = todo_norm_com[len('市'):]
                if len(todo_norm_com.decode('utf8')) >= 2:
                    norm_com = todo_norm_com
                    norm_coms.append(norm_com)
        for postfix in ['网', '股份有限公司', '有限责任公司', '有限公司', '公司', '集团']:
            if norm_com.endswith(postfix):
                todo_norm_com = norm_com[:len(norm_com)-len(postfix)]
                if len(todo_norm_com.decode('utf8')) >= 2:
                    norm_com = todo_norm_com
                    norm_coms.append(norm_com)
        if batch_result: return norm_coms
        else: return norm_com

    def com_to_id(self, cname):
        cname = self.com_format(cname)
        if self.name2id.has_key(cname):
            return self.name2id[cname]
        cname_norm = self.com_norm(cname)
        if cname != cname_norm:
            if self.name2id.has_key(cname_norm):
                return self.name2id[cname_norm]
        com_exts = self.extract_extensions(cname)
        for com_ext in com_exts:
            if self.name2id.has_key(com_ext):
                return self.name2id[com_ext]
        return -1

    def id_to_com(self, cid):
        cid = int(cid)
        if self.id2names.has_key(cid):
            return self.id2names[cid]
        else:
            return set()

    def extract_extensions(self, cname):
        ext_cnames = []
        tokens = []
        if cname.endswith(')'):
            tokens = cname.strip(')').split('(')
        elif cname.find('/') != -1:
            tokens = cname.split('/')
        elif cname.find('-') != -1:
            tokens = [cname.split('-')[0],'']
        elif cname.find('·') != -1:
            tokens = [cname.split('·')[0],'']
        else:
            tokens = CompanyNormalizer.seperate_chn_and_eng(cname)
        if len(tokens) == 2:
            for token in tokens:
                if len(token.decode('utf8')) < 2: continue
                if token in ['中国','北京','上海', '特殊普通合伙', '公司', '集团']: continue
                ext_cnames.append(token)
                token_pieces = CompanyNormalizer.seperate_chn_and_eng(token)
                for token_piece in token_pieces:
                    ext_cnames.append(token_piece)
        return filter(lambda x: x not in self.filter_list, map(lambda x: x.strip(), ext_cnames))

    @staticmethod
    def seperate_chn_and_eng(cname):
        parts = []
        chn_pattern = re.compile(ur'[\u4e00-\u9fa5]+')
        cname = cname.decode('utf8')
        if len(chn_pattern.findall(cname)) == 1:
            chn_part = chn_pattern.search(cname)
            (start,end) = chn_part.span()
            if start == 0 or end == len(cname):
                chn_part_len = end - start
                if chn_part_len >= 2 and len(cname) - chn_part_len >= 4:
                    for part in (cname[:start], cname[start:end], cname[end:]):
                        if len(part) > 0:
                            parts.append(part.encode('utf8'))
        return parts

if __name__ == '__main__':
    #com_normalizer = CompanyNormalizer('id_name_url_app.txt', 'com_name2id.txt', 'segment/data/')
    #for cname,cid in com_normalizer.name2id.items():
    #    print '%s\t%s' % (cname, cid)
    com_normalizer = CompanyNormalizer('id_name_url_app.txt', 'com_name2id.txt', 'segment/data/')
    print com_normalizer.com_norm(sys.argv[1])
    print com_normalizer.extract_extensions(sys.argv[1])
    print com_normalizer.com_to_id(sys.argv[1])
