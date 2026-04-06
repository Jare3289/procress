/**
 * ระบบบริหารจัดการประมวลผลเข้าชั้นเรียน (Merit-Based Class Allocation System)
 * Backend Data Persistence & Synchronization
 * Architecture: Serial Dictatorship (Merit-First)
 */

function formatThaiPhone(phone, forSheet = false) {
  if (!phone) return "";
  let digits = String(phone).replace(/\D/g, ''); 
  if (digits.length === 9 && !digits.startsWith('0')) {
    digits = '0' + digits;
  }
  if (forSheet && digits != "") {
    return "'" + digits; // บังคับเป็น Text ใน Google Sheets เพื่อรักษาเลข 0
  }
  return digits;
}

function doGet() {
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle('ระบบบริหารจัดการประมวลผลเข้าชั้นเรียน (MCAS)')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getAvailableVersions() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  let versions = [];
  
  const rawSheet = ss.getSheetByName('Sheet1') || ss.getSheets()[0];
  if (rawSheet) {
    versions.push({ id: 'raw_M1', name: 'ดึงข้อมูลดิบ ม.1 (จาก Sheet1)', sheetName: rawSheet.getName(), type: 'raw', level: 'M1' });
    versions.push({ id: 'raw_M4', name: 'ดึงข้อมูลดิบ ม.4 (จาก Sheet1)', sheetName: rawSheet.getName(), type: 'raw', level: 'M4' });
  }
  
  sheets.forEach(s => {
    const name = s.getName();
    if (name.startsWith('M1_Master_') || name.startsWith('M4_Master_')) {
      let lvl = name.substring(0, 2);
      versions.push({
        id: name,
        name: `ประวัติ ${lvl}: ` + name.replace(`${lvl}_Master_`, ''),
        sheetName: name,
        type: 'processed',
        level: lvl
      });
    }
  });
  return versions;
}

/**
 * ดึงข้อมูลดิบจาก Sheet ตาม type ('raw' หรือ 'processed')
 * 
 * โครงสร้าง raw (Sheet1):
 * 0: เลขประจำตัวผู้เข้าสอบ, 1: ชื่อ-สกุล, 2: โรงเรียนเดิม, 3: ชั้น, 4: ประเภทการสมัคร
 * 5: S, 6: T, 7: M, 8: L (อันดับแผน 1-4)
 * 9: คณิต, 10: วิทย์, 11: ไทย, 12: สังคม, 13: อังกฤษ, 14: อ่านฟัง
 * 15: เกรดเฉลี่ยรวม, 16: หมายเหตุ
 * 
 * โครงสร้าง processed (Master):
 * 0: ลำดับ, 1: เลขประจำตัว, 2: ชื่อ, 3: โรงเรียน, 4: ระดับชั้น, 5: ประเภทสมัคร
 * 6: แผน1, 7: แผน2, 8: แผน3, 9: แผน4, 10: เกรด
 * 11: คณิต, 12: วิทย์, 13: ไทย, 14: สังคม, 15: อังกฤษ, 16: อ่านฟัง
 * ...
 */
function loadDataByVersion(sheetName, type, levelStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName(sheetName);
  
  if (!sourceSheet) return [];

  const lastRow = sourceSheet.getLastRow();
  if (lastRow < 2) return []; 
  
  const rawValues = sourceSheet.getDataRange().getValues();
  const headers = rawValues[0] || [];
  let oldIdColIdx = headers.findIndex(h => String(h).match(/รหัสเดิม|รหัสประจำตัวเดิม/i));
  
  // Find GPA columns by headers
  let findCol = (pattern) => headers.findIndex(h => String(h).match(pattern));
  let gpaCols = {
    math: findCol(/เกรดคณิต|GPA คณิต|คะแนนเฉลี่ยคณิต/i),
    sci: findCol(/เกรดวิทย์|GPA วิทย์|คะแนนเฉลี่ยวิทย์|GPA Sci/i),
    thai: findCol(/เกรดไทย|GPA ไทย|คะแนนเฉลี่ยไทย/i),
    soc: findCol(/เกรดสังคม|GPA สังคม|คะแนนเฉลี่ยสังคม/i),
    eng: findCol(/เกรดอังกฤษ|GPA อังกฤษ|คะแนนเฉลี่ยอังกฤษ|GPA Eng/i)
  };

  const studentList = [];
  
  // -- Pre-fetch phones from Sheet1 as fallback --
  let rawPhoneMap = new Map();
  const rawSheet = ss.getSheetByName('Sheet1');
  let phoneIdxInRaw = -1;
  if (rawSheet) {
      const rawSheetValues = rawSheet.getDataRange().getValues();
      const rawHeaders = rawSheetValues[0] || [];
      phoneIdxInRaw = rawHeaders.findIndex(h => {
          const head = String(h).toLowerCase();
          return head.includes('เบอร์') || head.includes('โทร') || head.includes('phone') || head.includes('mobile') || head.includes('tel');
      });
      // Fallback to column Q (16) if not found by name
      const actualPhoneIdx = phoneIdxInRaw !== -1 ? phoneIdxInRaw : 16;

      for (let r = 1; r < rawSheetValues.length; r++) {
          let r_id = rawSheetValues[r][0] ? rawSheetValues[r][0].toString().trim() : "";
          let r_phone = rawSheetValues[r][actualPhoneIdx] ? rawSheetValues[r][actualPhoneIdx].toString().trim() : "";
          if (r_id) rawPhoneMap.set(r_id, r_phone);
      }
      // Keep track of the resolved index for the loop below if type is 'raw'
      phoneIdxInRaw = actualPhoneIdx;
  }
  
  for (let i = 1; i < rawValues.length; i++) { 
    const row = rawValues[i];
    
    let id, name, school, appType, gpa, plan1, plan2, plan3, plan4;
    let math, sci, thai, soc, eng, readScore, levelCol, remarkRaw, phone;
    let gpaMath = 0, gpaSci = 0, gpaThai = 0, gpaSoc = 0, gpaEng = 0;
    
    if (type === 'processed') {
        id = row[1];
        name = row[2];
        school = row[3];
        levelCol = row[4];
        appType = row[5];
        gpa = Number(row[10]) || 0;
        math = Number(row[11]) || 0;
        sci = Number(row[12]) || 0;
        thai = Number(row[13]) || 0;
        soc = Number(row[14]) || 0;
        eng = Number(row[15]) || 0;
        readScore = Number(row[16]) || 0;
        
        // Try mapping from headers if available, else use default (placeholder was 0)
        gpaMath = gpaCols.math !== -1 ? Number(row[gpaCols.math]) || 0 : 0;
        gpaSci = gpaCols.sci !== -1 ? Number(row[gpaCols.sci]) || 0 : 0;
        gpaThai = gpaCols.thai !== -1 ? Number(row[gpaCols.thai]) || 0 : 0;
        gpaSoc = gpaCols.soc !== -1 ? Number(row[gpaCols.soc]) || 0 : 0;
        gpaEng = gpaCols.eng !== -1 ? Number(row[gpaCols.eng]) || 0 : 0;
        
        plan1 = row[6]; plan2 = row[7]; plan3 = row[8]; plan4 = row[9];
        remarkRaw = row[21] ? row[21].toString().trim() : "";
        phone = row[23] ? row[23].toString().trim() : "";
        
        // Always try to fallback to Sheet1 lookup by Student ID
        let sheet1Phone = rawPhoneMap.get(String(id).trim());
        if (!phone || phone === "-" || phone === "") {
            phone = formatThaiPhone(sheet1Phone || "");
        } else {
            phone = formatThaiPhone(phone);
        }
    } else {
        // Raw data from Sheet1
        id = row[0] ? row[0].toString().trim() : "";
        name = row[1] ? row[1].toString().trim() : "";
        school = row[2] ? row[2].toString().trim() : "-";
        
        // Data Sanitization: Convert 5-digit codes to school name
        if (/^\d{5}$/.test(school)) {
            school = "ชัยนาทพิทยาคม";
        }

        levelCol = row[3] ? row[3].toString().trim() : ""; 
        let rawAppType = row[4] ? row[4].toString().trim() : ""; 
        
        // แปลงประเภทการสมัครให้ตรงกับชื่อโควตาที่ใช้ในการตั้งค่า
        appType = "ทั่วไป";
        if (rawAppType.includes("นอกเขต")) appType = "นอกเขต";
        else if (rawAppType.includes("ในเขต")) appType = "ในเขต";
        else if (rawAppType.includes("ความสามารถพิเศษ")) appType = "พิเศษ";
        else if (rawAppType.includes("เงื่อนไขพิเศษ")) appType = "เงื่อนไข";
        else if (rawAppType.includes("เดิม")) appType = "เดิม";
        else if (rawAppType.includes("อื่น")) appType = "อื่น";
        else if (rawAppType !== "") appType = rawAppType;
        
        // แผนการเรียน 1-4 (คอลัมน์ 5-8 เก็บเป็นอันดับ)
        let choiceMap = [];
        if (row[5]) choiceMap.push({ plan: "วิทยาศาสตร์-คณิตศาสตร์", rank: Number(row[5]) });
        if (row[6]) choiceMap.push({ plan: "วิทยาศาสตร์พลังสิบ", rank: Number(row[6]) });
        if (row[7]) choiceMap.push({ plan: "ศิลป์-คำนวณ", rank: Number(row[7]) });
        if (row[8]) choiceMap.push({ plan: "ศิลป์-ภาษา", rank: Number(row[8]) });
        
        choiceMap.sort((a, b) => a.rank - b.rank);
        
        plan1 = choiceMap[0] ? choiceMap[0].plan : "";
        plan2 = choiceMap[1] ? choiceMap[1].plan : "";
        plan3 = choiceMap[2] ? choiceMap[2].plan : "";
        plan4 = choiceMap[3] ? choiceMap[3].plan : "";
        
        math = Number(row[9]) || 0;
        sci  = Number(row[10]) || 0;
        thai = Number(row[11]) || 0;
        soc  = Number(row[12]) || 0;
        eng  = Number(row[13]) || 0;
        readScore = Number(row[14]) || 0;
        gpa = Number(row[15]) || 0;
        
        gpaEng = gpaCols.eng !== -1 ? Number(row[gpaCols.eng]) || 0 : 0;
        
        phone = formatThaiPhone(row[phoneIdxInRaw] || "");
        remarkRaw = row[17] ? row[17].toString().trim() : "";

        if (levelStr && levelCol !== levelStr && levelCol !== "") continue;
    }

    if (!id) continue;
    let oldStudentId = oldIdColIdx !== -1 ? (row[oldIdColIdx] ? String(row[oldIdColIdx]).trim() : "") : "";
    
    // Map "ทั่วไป" to appropriate default based on level
    if (appType === "ทั่วไป" || appType.includes("นักเรียนทั่วไป")) {
        let isM1 = (levelStr === 'M1' || levelStr === 'ม.1');
        appType = isM1 ? "นอกเขต" : "เดิม";
    }

    studentList.push({
      id: id,
      name: name || `นักเรียน ${id}`,
      school: school,
      appType: appType,
      gpa: gpa,
      gpas: { math: gpaMath, sci: gpaSci, thai: gpaThai, soc: gpaSoc, eng: gpaEng },
      choices: [plan1, plan2, plan3, plan4].filter(p => p !== ""),
      scores: { math: math, sci: sci, thai: thai, soc: soc, eng: eng, read: readScore },
      levelCol: levelCol,
      status: "ไม่ผ่านการจัดสรร", 
      assignedPlan: "หลุดอันดับ (รอจัดสรร)",
      assignedRoom: "-",
      phone: phone,
      remark: remarkRaw,
      oldStudentId: oldStudentId
    });
  }

  return studentList;
}

/**
 * ฟังก์ชันบันทึกคะแนนดิบกลับลง Google Sheet
 * รับข้อมูลจาก Frontend ที่ผู้ใช้กรอก/อัปโหลดผ่าน Excel
 */
function saveRawScores(dataToSave, sheetName, type) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  
  if (!sheet) return `ไม่พบแผ่นงาน ${sheetName} ในฐานข้อมูลต้นฉบับ`;
  
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return "ไม่มีข้อมูลนักเรียนให้บันทึก";
  
  // Auto-detect type if not provided
  if (!type) {
    const headers = sheet.getRange(1, 1, 1, 3).getValues()[0];
    if (String(headers[0]).trim() === 'ลำดับ' && String(headers[1]).trim() === 'เลขประจำตัวผู้เข้าสอบ') {
       type = 'processed';
    } else {
       type = 'raw';
    }
  }
  
  // Raw: ID=col0(A), scores start at col9(J): คณิต, วิทย์, ไทย, สังคม, อังกฤษ, อ่านฟัง (6 cols)
  // Processed: ID=col1(B), scores start at col11(L): คณิต, วิทย์, ไทย, สังคม, อังกฤษ, อ่านฟัง (6 cols)
  let idCol = 1;         // Column A for raw (1-indexed)
  let scoreColStart = 10; // Column J for raw (1-indexed)
  let numScoreCols = 6;   // คณิต, วิทย์, ไทย, สังคม, อังกฤษ, อ่านฟัง
  
  if (type === 'processed') {
      idCol = 2;           // Column B for Master
      scoreColStart = 12;  // Column L for Master
  }
  
  // Use getDisplayValues to ensure IDs are strings (avoid scientific notation)
  const idRange = sheet.getRange(2, idCol, lastRow - 1, 1).getDisplayValues();
  const scoreRange = sheet.getRange(2, scoreColStart, lastRow - 1, numScoreCols); 
  const currentScores = scoreRange.getValues();
  
  const dataMap = new Map();
  dataToSave.forEach(s => {
    if (s && s.id !== undefined && s.id !== null) {
      dataMap.set(String(s.id).trim(), s);
    }
  });
  
  let updatedCount = 0;
  for (let i = 0; i < idRange.length; i++) {
    let sid = idRange[i][0] ? idRange[i][0].toString().trim() : "";
    let student = dataMap.get(sid);
    if (student) {
       // Order: คณิต, วิทย์, ไทย, สังคม, อังกฤษ, อ่านฟัง
       if (student.scores.math !== undefined) currentScores[i][0] = student.scores.math;
       if (student.scores.sci !== undefined) currentScores[i][1] = student.scores.sci;
       if (student.scores.thai !== undefined) currentScores[i][2] = student.scores.thai;
       if (student.scores.soc !== undefined) currentScores[i][3] = student.scores.soc;
       if (student.scores.eng !== undefined) currentScores[i][4] = student.scores.eng;
       if (student.scores.read !== undefined) currentScores[i][5] = student.scores.read;
       updatedCount++;
    }
  }
  
  if (updatedCount === 0) {
      let firstFewSheetIds = idRange.slice(0, 3).map(r => r[0]).join(", ");
      let firstFewMapIds = Array.from(dataMap.keys()).slice(0, 3).join(", ");
      return `อัปเดตได้ 0 รายการ (ระบบหา ID ใน Sheet ไม่ตรงกับบนเว็บ)
ID ในชีต: [${firstFewSheetIds}] 
ID บนเว็บ: [${firstFewMapIds}]
กรุณาตรวจสอบว่าเลือกดึงฐานข้อมูลถูกระดับชั้นหรือไม่`;
  }
  
  scoreRange.setValues(currentScores);
  SpreadsheetApp.flush();
  return `อัปเดตคะแนนจำนวน ${updatedCount} รายการ ลงแผ่นงาน ${sheetName} สำเร็จแล้ว`;
}

/**
 * ประมวลผลจัดสรร (เรียกจาก Frontend)
 * คำนวณคะแนนรวม, จัดเรียง, ตัดโควตา, จัดห้อง S-Curve
 */
function processAllocation(students, quotaSettings, planSettings, levelStr, roomSettings, maxScores, weights) {
  const isM4 = (levelStr === 'M4' || levelStr === 'ม.4');
  const subjRankList = isM4 ? ['math', 'sci', 'thai', 'eng'] : ['math', 'sci', 'thai', 'soc', 'eng'];
  
  students.forEach(s => {
    let totalEarned = 0;
    let totalMax = 0;
    
    subjRankList.forEach(k => {
        totalEarned += (s.scores[k] || 0);
        totalMax += (maxScores[k] || 100);
    });
    
    if (totalMax > 0) {
        s.totalScore = (totalEarned / totalMax) * 100;
    } else {
        s.totalScore = 0;
    }
  });
  
  const sortStudents = (a, b) => {
    // Stage 1: Total Score (normalized sum)
    if (Math.abs(b.totalScore - a.totalScore) > 0.000001) return b.totalScore - a.totalScore;
    
    // Stage 2: Individual Subject Scores (Exam) in order
    for (let sKey of subjRankList) {
        if ((b.scores[sKey]||0) !== (a.scores[sKey]||0)) return (b.scores[sKey]||0) - (a.scores[sKey]||0);
    }
    
    // Stage 3: GPA (Average 2 years for M1, 5 terms for M4)
    if (Math.abs((b.gpa||0) - (a.gpa||0)) > 0.000001) return (b.gpa||0) - (a.gpa||0);
    
    // Stage 4: GPA Sub-subjects in order
    if (a.gpas && b.gpas) {
      for (let sKey of subjRankList) {
          if ((b.gpas[sKey]||0) !== (a.gpas[sKey]||0)) return (b.gpas[sKey]||0) - (a.gpas[sKey]||0);
      }
    }
    
    return 0;
  };

  // Sort all students by score desc for within-group priority
  students.sort(sortStudents);
  
  // Assign systemRank mapping
  students.forEach((s, idx) => s.systemRank = idx + 1);

  // Note tie-breaker rationale as side-effect so users see why someone won
  for (let i = 1; i < students.length; i++) {
     let prev = students[i-1];
     let curr = students[i];
     if (Math.abs(prev.totalScore - curr.totalScore) < 0.000001) {
         let reason = "";
         const labels = { math: "คณิตศาสตร์", sci: "วิทยาศาสตร์", thai: "ภาษาไทย", soc: "สังคมศึกษา", eng: "ภาษาอังกฤษ" };
         
         // Test Stage 2: Individual exam scores
         for (let k of subjRankList) {
            if ((prev.scores[k]||0) !== (curr.scores[k]||0)) {
               reason = `คะแนน${labels[k]}ดีกว่า`;
               break;
            }
         }
         
         // Test Stage 3: Total GPA
         if (!reason && Math.abs((prev.gpa||0) - (curr.gpa||0)) > 0.000001) {
            reason = isM4 ? "ผลการเรียนเฉลี่ย ๕ ภาคเรียนดีกว่า" : "ผลการเรียนเฉลี่ย (GPA) สูงกว่า";
         }
         
         // Test Stage 4: Individual GPA
         if (!reason && prev.gpas && curr.gpas) {
            for (let k of subjRankList) {
               if ((prev.gpas[k]||0) !== (curr.gpas[k]||0)) {
                  reason = `ผลการเรียนรายวิชา${labels[k]}สูงกว่า`;
                  break;
               }
            }
         }

         if (reason) {
             let rz = "[คะแนนเท่ากับ " + (curr.name || curr.id) + " ชนะด้วย" + reason + "]";
             prev.remark = (prev.remark ? prev.remark + " | " : "") + rz;
             
             let rzcurr = "[คะแนนเท่ากับ " + (prev.name || prev.id) + " แต่พ่ายแพ้เนื่องจาก" + reason.replace('กว่า', 'น้อยกว่า') + "]";
             curr.remark = (curr.remark ? curr.remark + " | " : "") + rzcurr;
         } else {
             let rx = "[คะแนนเท่ากันทุกวิชากับ " + (curr.name || curr.id) + "]";
             prev.remark = (prev.remark ? prev.remark + " | " : "") + rx;
             let rxcurr = "[คะแนนเท่ากันทุกวิชากับ " + (prev.name || prev.id) + "]";
             curr.remark = (curr.remark ? curr.remark + " | " : "") + rxcurr;
         }
     }
  }

  // 2. State Initialization
  let plansLeft = {};
  for(let p in planSettings) plansLeft[p] = planSettings[p] || 0;

  let quotasLeft = {
    "ในเขต": quotaSettings["ในเขต"] || 0,
    "นอกเขต": quotaSettings["นอกเขต"] || 0,
    "พิเศษ": quotaSettings["พิเศษ"] || 0,
    "เงื่อนไข": quotaSettings["เงื่อนไข"] || 0,
    "เดิม": quotaSettings["เดิม"] || 0,
    "อื่น": quotaSettings["อื่น"] || 0
  };

  // 3. PASS 1: SCHOOL ADMISSION (Determine WHO gets into the school)
  if (isM1) {
    // M.1 Rule: Rank everyone together.
    // Special Talent ("พิเศษ") and Conditional ("เงื่อนไข") are guaranteed admission regardless of quota.
    students.sort((a, b) => a.systemRank - b.systemRank);
    students.forEach(s => {
      if (s.appType === "พิเศษ" || s.appType === "เงื่อนไข") {
        s._admitted = true;
        s._admissionQuota = `ตัวจริง (${s.appType})`;
        // Do not decrement quota counters for guaranteed types.
      } else if (quotasLeft[s.appType] > 0) {
        s._admitted = true;
        s._admissionQuota = `ตัวจริง (${s.appType})`;
        quotasLeft[s.appType]--;
      }
    });

  } else {
    // M.4 Rule: Rank by Tiers
    const markGroup = (types, prefix) => {
      let group = students.filter(s => types.includes(s.appType) && !s._admitted);
      group.sort((a, b) => a.systemRank - b.systemRank);
      group.forEach(s => {
        if (quotasLeft[s.appType] > 0) {
          s._admitted = true;
          s._admissionQuota = prefix + ` (${s.appType})`;
          quotasLeft[s.appType]--;
        }
      });
    };

    // Admission priority for M.4: Internal, Special, Condition
    markGroup(["เดิม"], "ตัวจริง");
    markGroup(["พิเศษ", "เงื่อนไข"], "ตัวจริง");

    // After priority groups, fill the remaining seats with "Others" (จากโรงเรียนอื่น)
    let totalCapacity = 0;
    for (let p in planSettings) totalCapacity += planSettings[p] || 0;
    
    let admittedSoFar = students.filter(s => s._admitted).length;
    let seatsRemaining = totalCapacity - admittedSoFar;

    if (seatsRemaining > 0) {
      let others = students.filter(s => s.appType === "อื่น" && !s._admitted);
      others.sort((a, b) => a.systemRank - b.systemRank);
      others.forEach(s => {
        if (seatsRemaining > 0) {
          s._admitted = true;
          s._admissionQuota = "ตัวจริง (อื่น)";
          seatsRemaining--;
        }
      });
    }
  }

  // 4. PASS 2: PLAN ALLOCATION (Determine WHICH PLAN they get)
  let admittedStudents = students.filter(s => s._admitted);

  // Auto-fill waitlisters to strictly reach exact capacity (so every room can be 40)
  let totalCap = 0;
  for (let p in planSettings) totalCap += (planSettings[p] || 0);

  if (admittedStudents.length < totalCap) {
    let toPromote = totalCap - admittedStudents.length;
    let availableWaitlist = students.filter(s => !s._admitted && !((s.queueStatus || s.status || "").includes("สละสิทธิ์")));
    availableWaitlist.sort((a,b) => b.totalScore - a.totalScore);
    
    let grab = availableWaitlist.slice(0, toPromote);
    grab.forEach(s => {
      s._admitted = true;
      s._admissionQuota = "ตัวสำรอง (เรียกเพิ่มอุดที่นั่งว่าง)";
      admittedStudents.push(s);
    });
  }

  // According to rule 6.2/6.3, all admitted students pick their plans simultaneously 
  // based ENTIRELY on their systemRank (total score descending), regardless of their quota type.
  // HOWEVER, waitlisters promoted to fill gaps MUST pick last after all real students.
  let allocationOrder = [...admittedStudents];
  allocationOrder.sort((a, b) => {
    let aIsWait = (a._admissionQuota || "").includes("สำรอง");
    let bIsWait = (b._admissionQuota || "").includes("สำรอง");
    if (aIsWait && !bIsWait) return 1;
    if (!aIsWait && bIsWait) return -1;
    return a.systemRank - b.systemRank;
  });

  // Allocate plans
  allocationOrder.forEach(s => {
    let placed = false;
    for (let choice of s.choices) {
      if (plansLeft[choice] > 0) {
        plansLeft[choice]--;
        s.assignedPlan = choice;
        s.status = s._admissionQuota || "ตัวจริง";
        placed = true;
        break;
      }
    }
    if (!placed) {
      // If none of their choices are available, they are technically admitted but waiting.
      // Will put them in any leftover plan.
      for (let p in plansLeft) {
        if (plansLeft[p] > 0) {
          plansLeft[p]--;
          s.assignedPlan = p;
          let isWaitlist = (s._admissionQuota || "").includes("สำรอง");
          s.status = isWaitlist ? "ตัวสำรอง (ถูกปัดไปแผนที่ว่าง)" : "ตัวจริง (ถูกปัดไปแผนที่ว่าง)";
          placed = true;
          break;
        }
      }
      if (!placed) {
         // Should not happen if total capacity = sum of plans
         s._admitted = false;
      }
    }
  });

  // 5. Mark Non-Admitted
  students.forEach(s => {
    if (!s._admitted) {
      s.status = "สำรอง";
      s.assignedPlan = "หลุดอันดับ (รอจัดสรร)";
      s.assignedRoom = "-";
    }
  });

  // 5. S-Curve Room Distribution (Strictly 40 per room)
  let admittedByPlan = {
      "วิทยาศาสตร์-คณิตศาสตร์": [],
      "วิทยาศาสตร์พลังสิบ": [],
      "ศิลป์-คำนวณ": [],
      "ศิลป์-ภาษา": []
  };

  students.forEach(s => {
      if (s._admitted) admittedByPlan[s.assignedPlan].push(s);
  });

  let roomCounter = 1;
  const planOrderList = ["วิทยาศาสตร์-คณิตศาสตร์", "วิทยาศาสตร์พลังสิบ", "ศิลป์-คำนวณ", "ศิลป์-ภาษา"];
  
  planOrderList.forEach(plan => {
      let planStudents = admittedByPlan[plan] || [];
      planStudents.sort((a, b) => b.totalScore - a.totalScore);
      
      let numRooms = roomSettings && roomSettings[plan] ? roomSettings[plan] : 0;
      let roomsInPlan = [];
      if (numRooms > 0) {
          let capacity = numRooms * 40;
          // Fill rooms linearly (max 40 per room by default)

          // Fill rooms linearly (max 40 per room by default)
          // Top 40 go to the first room, next 40 to the second, etc.
          // Real students were already sorted at the top. 
          // Waitlist students were appended to the very end, so they take the last vacant spots naturally.
          planStudents.forEach((s, i) => {
              let roomIndex = Math.floor(i / 40);
              if (roomIndex >= numRooms) {
                  // Overflow case: append to the very last room
                  roomIndex = numRooms - 1;
              }
              s.assignedRoom = `ห้อง ${roomsInPlan[roomIndex]}`;
          });
      }
  });

  // 6. Unified Waitlist Rank Calculation
  let totalWaitlist = students.filter(s => !s._admitted).sort((a, b) => b.totalScore - a.totalScore);
  totalWaitlist.forEach((s, idx) => {
      s._waitlistRank = idx + 1;
  });

  // 7. Data Return
  return {
    allStudents: students.map(s => ({
      id: s.id,
      name: s.name,
      school: s.school,
      systemRank: s.systemRank,
      choices: s.choices,
      appType: s.appType,
      totalScore: s.totalScore,
      totalScore: s.totalScore,
      math: (s.scores ? s.scores.math : (s.math || 0)),
      sci: (s.scores ? s.scores.sci : (s.sci || 0)),
      thai: (s.scores ? s.scores.thai : (s.thai || 0)),
      soc: (s.scores ? s.scores.soc : (s.soc || 0)),
      eng: (s.scores ? s.scores.eng : (s.eng || 0)),
      read: (s.scores ? s.scores.read : (s.read || 0)),
      gpa: s.gpa,
      gpas: s.gpas,
      levelCol: s.levelCol,
      status: s.status,
      assignedPlan: s.assignedPlan,
      assignedRoom: s.assignedRoom,
      phone: s.phone || "",
      remark: s.remark || "",
      oldStudentId: s.oldStudentId || "",
      waitlistRank: s._waitlistRank || 0
    })),
    waitlists: {
      "รวมทั้งหมด": students.filter(s => !s._admitted).sort((a,b) => b.totalScore - a.totalScore)
    }
  };
}

/**
 * Phase 1: บันทึกบัญชีจัดแถว (Merit List) แยก 3 ชีต ตามคำขอ
 * 1. Consolidated (ตัวจริง + สำรองกลุ่ม)
 * 2. Group 1 (ในเขต + พิเศษ + เงื่อนไข)
 * 3. Group 2 (นอกเขต)
 */
function saveQueueList(dataToSave, levelStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const prefix = levelStr ? `${levelStr}_` : '';
  const headers = [
    "ลำดับระบบ", "กลุ่ม", "สถานะ", "เลขประจำตัวผู้เข้าสอบ", "ชื่อ-นามสกุล", "โรงเรียนเดิม", "ประเภทการสมัคร", 
    "คะแนนรวม(%)", "S", "T", "M", "L",
    "คะแนนคณิต", "คะแนนวิทย์", "คะแนนไทย", "คะแนนสังคม", "คะแนนอังกฤษ", "คะแนนอ่านออกเสียง",
    "ส", "หมายเหตุ", "รหัสประจำตัวเดิม", "เบอร์โทร"
  ];

  const pNames = ["วิทยาศาสตร์-คณิตศาสตร์", "วิทยาศาสตร์พลังสิบ", "ศิลป์-คำนวณ", "ศิลป์-ภาษา"];

  const getRow = (s) => {
    let choices = s.choices || s._rawChoices || [];
    let getRank = (pName) => {
      let idx = choices.indexOf(pName);
      return idx !== -1 ? idx + 1 : '-';
    };
    let allocationStatus = s.assignedRoom || s.assignedPlan || (s.isAdmitted ? "ตัวจริง (รอจัดห้อง)" : "-");
    if (s._roomCode) allocationStatus = s._roomCode;

    return [
      s.systemRank || "-", s.queueGroup || "-", s.status || s.queueStatus || "-", s.id, s.name, s.school, s.appType || "-",
      s.totalScore, getRank(pNames[0]), getRank(pNames[1]), getRank(pNames[2]), getRank(pNames[3]),
      s.math || 0, s.sci || 0, s.thai || 0, s.soc || 0, s.eng || 0, s.read || 0,
      allocationStatus, s.remark || "-", s.oldStudentId || "", formatThaiPhone(s.phone || "", true)
    ];
  };

  const phaserSort = (list) => {
    return [...list].sort((a, b) => {
      const aIsWaived = (a.queueStatus || a.status || "").includes("สละสิทธิ์");
      const bIsWaived = (b.queueStatus || b.status || "").includes("สละสิทธิ์");
      if (aIsWaived && !bIsWaived) return 1;
      if (!aIsWaived && bIsWaived) return -1;
      return (a.systemRank || 999999) - (b.systemRank || 999999);
    });
  };

  const writeToSheet = (name, list, bgColor) => {
    let sheet = ss.getSheetByName(name);
    if (!sheet) {
      sheet = ss.insertSheet(name);
    } else {
      sheet.clear();
      // Restore column width/formatting if needed, but clear is usually enough
    }
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]).setFontWeight("bold").setBackground(bgColor).setFontColor("white");
    const rows = phaserSort(list).map(getRow);
    if (rows.length > 0) {
      sheet.getRange(2, 1, rows.length, headers.length).setValues(rows);
      sheet.setFrozenRows(1);
    }
    return sheet;
  };

  // 1. Consolidated
  writeToSheet(`${prefix}Merit_Consolidated`, dataToSave, "#0f172a");

  // 2. Group 1
  const listG1 = dataToSave.filter(s => (s.queueGroup || "").includes("ในเขต") || (s.queueGroup || "").includes("เดิม") || s.appType === "พิเศษ" || s.appType === "เงื่อนไข");
  writeToSheet(`${prefix}Merit_Group1`, listG1, "#1e3a8a");

  // 3. Group 2
  const listG2 = dataToSave.filter(s => (s.queueGroup || "").includes("นอกเขต") || (s.queueGroup || "").includes("อื่น"));
  writeToSheet(`${prefix}Merit_Group2`, listG2, "#1e40af");

  return "บันทึกทับไฟล์เดิม 3 บัญชีเรียบร้อย (Consolidated, Group1, Group2)";
}

/**
 * ดึงข้อมูล Merit List ล่าสุด เพื่อเข้าสู่ Phase 2 ทันที
 */
function getLatestMerit(levelStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const prefix = levelStr ? `${levelStr}_` : '';
  const exactName = `${prefix}Merit_Consolidated`;
  
  let candidate = ss.getSheetByName(exactName);
  
  if (!candidate) {
    const sheets = ss.getSheets();
    const searchPattern = levelStr ? `${levelStr}_Merit_Consolidated_` : `Merit_Consolidated_`;
    candidate = sheets.filter(sh => sh.getName().includes(searchPattern))
                      .sort((a,b) => b.getName().localeCompare(a.getName()))[0];
  }
  
  if (!candidate) throw new Error(`ไม่พบบัญชีผลการเข้าแถว (${exactName}) ที่เคยบันทึกไว้`);
  
  const values = candidate.getDataRange().getValues();
  if (values.length < 2) return [];
  
  const headers = values[0];
  const pNames = ["วิทยาศาสตร์-คณิตศาสตร์", "วิทยาศาสตร์พลังสิบ", "ศิลป์-คำนวณ", "ศิลป์-ภาษา"];
  
  // -- Pre-fetch phones from Sheet1 as fallback --
  let rawPhoneMap = new Map();
  const rawSheet = ss.getSheetByName('Sheet1');
  if (rawSheet) {
      const rawSheetValues = rawSheet.getDataRange().getValues();
      const rawHeaders = rawSheetValues[0] || [];
      const phoneIdx = rawHeaders.findIndex(h => String(h).match(/เบอร์โทร|โทรศัพท์|Phone|เบอร์สายตรง/i));
      const targetIdx = phoneIdx !== -1 ? phoneIdx : 16;
      for (let r = 1; r < rawSheetValues.length; r++) {
          let r_id = rawSheetValues[r][0] ? rawSheetValues[r][0].toString().trim() : "";
          let r_phone = rawSheetValues[r][targetIdx] ? rawSheetValues[r][targetIdx].toString().trim() : "";
          if (r_id) rawPhoneMap.set(r_id, r_phone);
      }
  }
  
  return values.slice(1).map(row => {
    // Mapping back based on fixed headers
    let _id = row[3] ? row[3].toString().trim() : "";
    let _phone = formatThaiPhone(row[21] || "");
    if (!_phone && rawPhoneMap.has(_id)) _phone = formatThaiPhone(rawPhoneMap.get(_id));
    // Note: getLatestMerit is for UI load, so no quote here.


    let student = {
      systemRank: parseInt(row[0]),
      queueGroup: row[1],
      status: row[2],
      queueStatus: row[2],
      id: row[3],
      name: row[4],
      school: row[5],
      appType: row[6],
      totalScore: parseFloat(row[7]),
      isAdmitted: row[2].includes("ตัวจริง"),
      choices: [],
      math: parseFloat(row[12]) || 0,
      sci: parseFloat(row[13]) || 0,
      thai: parseFloat(row[14]) || 0,
      soc: parseFloat(row[15]) || 0,
      eng: parseFloat(row[16]) || 0,
      read: parseFloat(row[17]) || 0,
      remark: row[19],
      oldStudentId: row[20] || "",
      phone: _phone
    };
    
    student.scores = {
      math: student.math,
      sci: student.sci,
      thai: student.thai,
      soc: student.soc,
      eng: student.eng,
      read: student.read
    };
    
    // Reconstruct choices from 1,2,3,4 ranks in sheet
    let choiceArray = [null, null, null, null];
    [8,9,10,11].forEach((colIdx, i) => {
      let val = parseInt(row[colIdx]);
      if (!isNaN(val) && val >= 1 && val <= 4) {
        choiceArray[val-1] = pNames[i];
      }
    });
    student.choices = choiceArray.filter(c => c !== null);
    
    return student;
  });
}

/**
 * บันทึกเวอร์ชันและผลลัพธ์ลง Spreadsheet
 */
function saveNewVersion(dataToSave, levelStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const prefix = levelStr ? `${levelStr}_` : '';
  const masterSheetName = `${prefix}Master_Processed_Final`;
  
  let masterSheet = ss.getSheetByName(masterSheetName);
  if (!masterSheet) {
    masterSheet = ss.insertSheet(masterSheetName);
  } else {
    masterSheet.clear();
  }
  
  const standardHeaders = [
    "ลำดับ", "เลขประจำตัว", "ชื่อ-นามสกุล", "โรงเรียนเดิม", "ระดับชั้น", "ประเภทการสมัคร", 
    "S", "T", "M", "L", "เกรด (GPA)",
    "คณิต", "วิทย์", "ไทย", "สังคม", "อังกฤษ", "อ่านออกเสียง", 
    "คะแนนรวม(%)", "สถานะ", "แผนที่ได้", "ห้อง", "หมายเหตุ", "รหัสประจำตัวเดิม", "เบอร์โทร"
  ];
  
  masterSheet.getRange(1, 1, 1, standardHeaders.length).setValues([standardHeaders])
    .setFontWeight("bold").setBackground("#1e293b").setFontColor("white");
  
  const pS = "วิทยาศาสตร์-คณิตศาสตร์";
  const pT = "วิทยาศาสตร์พลังสิบ";
  const pM = "ศิลป์-คำนวณ";
  const pL = "ศิลป์-ภาษา";

  let masterRows = [standardHeaders];
  // Data was already sorted or we sort it here to be sure:
  let sortedMaster = [...dataToSave].sort((a, b) => (a.systemRank || 999999) - (b.systemRank || 999999));
  
  sortedMaster.forEach((s, idx) => {
    let choices = s.choices || s._rawChoices || [];
    let getChoiceRank = (pName) => {
      let idx = (s.choices || s._rawChoices || []).indexOf(pName);
      return idx !== -1 ? idx + 1 : '-';
    };

    masterRows.push([
      idx + 1, s.id, s.name, s.school, levelStr, s.appType,
      getChoiceRank(pS), getChoiceRank(pT), getChoiceRank(pM), getChoiceRank(pL),
      s.gpa,
      s.math, s.sci, s.thai, s.soc, s.eng, s.read !== undefined ? s.read : 0,
      s.totalScore, s.status, s.assignedPlan || "-", s.assignedRoom || "-", s.remark || "-", s.oldStudentId || "", formatThaiPhone(s.phone || "", true)
    ]);
  });
  
  if (masterRows.length > 0) {
      masterSheet.getRange(1, 1, masterRows.length, standardHeaders.length).setValues(masterRows);
  }


  // --- (2) สร้างชีตตัวจริงแยกตามห้อง (_Class_By_Room_Final) ---
  const classSheetName = `${prefix}Class_By_Room_Final`;
  let classSheet = ss.getSheetByName(classSheetName);
  if (!classSheet) {
    classSheet = ss.insertSheet(classSheetName);
  } else {
    classSheet.clear();
  }
  
  
  let classRows = [];
  classRows.push(standardHeaders);
  
  const planOrder = ["วิทยาศาสตร์-คณิตศาสตร์", "วิทยาศาสตร์พลังสิบ", "ศิลป์-คำนวณ", "ศิลป์-ภาษา"];
  let seatedStudents = dataToSave.filter(s => s.assignedRoom && s.assignedRoom !== "-" && s.assignedRoom !== "สำรอง");
  
  planOrder.forEach(planName => {
    let planStudents = seatedStudents.filter(s => s.assignedPlan === planName);
    if (planStudents.length > 0) {
      let r1 = new Array(standardHeaders.length).fill("");
      r1[0] = `[ แผนการเรียน: ${planName} ]`;
      classRows.push(r1);
      
      let roomsInPlan = Array.from(new Set(planStudents.map(s => s.assignedRoom))).sort();
      roomsInPlan.forEach(room => {
        let roomStudents = planStudents.filter(s => s.assignedRoom === room).sort((a, b) => b.totalScore - a.totalScore);
        let r2 = new Array(standardHeaders.length).fill("");
        r2[0] = `--- ห้อง: ${room} ---`;
        classRows.push(r2);

        roomStudents.forEach((s, idx) => {
          let getCRank = (pn) => {
            let cs = s.choices || s._rawChoices || [];
            let i = cs.indexOf(pn);
            return i !== -1 ? i + 1 : '-';
          };
          classRows.push([
            idx + 1, s.id, s.name, s.school, levelStr, s.appType,
            getCRank(pS), getCRank(pT), getCRank(pM), getCRank(pL),
            s.gpa,
            s.math, s.sci, s.thai, s.soc, s.eng, s.read !== undefined ? s.read : 0,
            s.totalScore,
            s.status, s.assignedPlan, s.assignedRoom, s.remark || '-', s.oldStudentId || '', formatThaiPhone(s.phone || '', true)
          ]);
        });
        classRows.push(new Array(standardHeaders.length).fill("")); 
      });
    }
  });

  if (classRows.length > 1) {
    classSheet.getRange(1, 1, classRows.length, standardHeaders.length).setValues(classRows);
    classSheet.getRange(1, 1, 1, standardHeaders.length).setBackground("#334155").setFontColor("white").setFontWeight("bold");
    classSheet.setFrozenRows(1);
    classSheet.setColumnWidth(2, 110);
    classSheet.setColumnWidth(3, 180);
    classSheet.setColumnWidth(20, 150);
    classSheet.setColumnWidth(21, 120);
    classSheet.setColumnWidth(22, 150);
  }

  // --- (3) สร้างชีตบัญชีสำรองรวม (_Waitlist_Final) ---
  const waitlistSheetName = `${prefix}Waitlist_Final`;
  let waitSheet = ss.getSheetByName(waitlistSheetName);
  if (!waitSheet) {
    waitSheet = ss.insertSheet(waitlistSheetName);
  } else {
    waitSheet.clear();
  }

  let waitRows = [];
  waitRows.push(standardHeaders);
  
  let globalWaitlist = dataToSave.filter(s => s.status === "สำรอง" || !s._admitted || s.assignedRoom === "-").sort((a,b) => b.totalScore - a.totalScore);

  if (globalWaitlist.length > 0) {
    let w1 = new Array(standardHeaders.length).fill("");
    w1[0] = `::: บัญชีสำรอง (จัดเรียงตามคะแนนรวม) :::`;
    waitRows.push(w1);
      
    globalWaitlist.forEach((s, idx) => {
      let getCRank = (pn) => {
        let cs = s.choices || s._rawChoices || [];
        let i = cs.indexOf(pn);
        return i !== -1 ? i + 1 : '-';
      };
      waitRows.push([
        idx + 1, s.id, s.name, s.school, levelStr, s.appType,
        getCRank(pS), getCRank(pT), getCRank(pM), getCRank(pL),
        s.gpa,
        s.math, s.sci, s.thai, s.soc, s.eng, s.read !== undefined ? s.read : 0,
        s.totalScore,
        "สำรอง", "-", "-", "รอเรียกตัว", s.oldStudentId || '', formatThaiPhone(s.phone || '', true)
      ]);
    });
    waitRows.push(new Array(standardHeaders.length).fill(""));
  }

  if (waitRows.length > 1) {
    waitSheet.getRange(1, 1, waitRows.length, standardHeaders.length).setValues(waitRows);
    waitSheet.getRange(1, 1, 1, standardHeaders.length).setBackground("#334155").setFontColor("white").setFontWeight("bold");
    waitSheet.setFrozenRows(1);
    waitSheet.setColumnWidth(2, 110);
    waitSheet.setColumnWidth(3, 180);
    waitSheet.setColumnWidth(22, 280); 
  }

  SpreadsheetApp.flush();
  return `บันทึกรายงานสำเร็จ! [ม.${levelStr === 'M4' ? '4' : '1'}]\n1. ${masterSheetName}\n2. ${classSheetName}\n3. ${waitlistSheetName}`;
}

/**
 * Phase 1: Process Queue
 * จัดลำดับเข้าแถวใหม่ แยกเป็น 2 กลุ่ม (กลุ่มเดิม/ในเขต และ กลุ่มอื่น/นอกเขต)
 */
function processQueue(students, quotaSettings, levelStr, maxScores) {
  let isM1 = (levelStr === 'M1' || levelStr === 'ม.1');
  
  // 1. คำนวณร้อยละและจัดเรียงคะแนนแบบเดียวกัน
  students.forEach(s => {
    let subjects = isM1 ? ['math', 'sci', 'thai', 'soc', 'eng', 'read'] : ['math', 'sci', 'thai', 'eng'];
    let totalEarned = 0;
    let totalMax = 0;
    
    subjects.forEach(k => {
        totalEarned += (s.scores[k] || 0);
        totalMax += (maxScores[k] || 100);
    });
    
    s.totalScore = totalMax > 0 ? (totalEarned / totalMax) * 100 : 0;
  });
  
  const sortStudents = (a, b) => {
    if (Math.abs(b.totalScore - a.totalScore) > 0.000001) return b.totalScore - a.totalScore;
    const subjOrder = isM1 ? ['math', 'sci', 'thai', 'soc', 'eng'] : ['math', 'sci', 'thai', 'eng'];
    for (let sKey of subjOrder) {
        if ((b.scores[sKey]||0) !== (a.scores[sKey]||0)) return (b.scores[sKey]||0) - (a.scores[sKey]||0);
    }
    if (Math.abs((b.gpa||0) - (a.gpa||0)) > 0.000001) return (b.gpa||0) - (a.gpa||0);
    return 0;
  };

  students.sort(sortStudents);
  students.forEach((s, idx) => s.systemRank = idx + 1);
  
  // Reset states
  students.forEach(s => {
    s._admitted = false;
    s.queueStatus = "รอจัดสรร";
    s.queueGroup = "-";
    s.queueRank = 0;
  });

  if (isM1) {
    // M.1 Logic
    let baseQuotaSpTalent = quotaSettings["พิเศษ"] || 0;
    let baseQuotaSpCond   = quotaSettings["เงื่อนไข"] || 0;
    let baseQuotaIn       = quotaSettings["ในเขต"] || 0;
    let baseQuotaOut      = quotaSettings["นอกเขต"] || 0;

    // 1. Special Talent
    let gSpTalent = students.filter(s => s.appType === "พิเศษ");
    gSpTalent.sort(sortStudents);
    let admittedSpTalent = 0;
    gSpTalent.forEach((s, i) => {
      s.queueGroup = "กลุ่มความสามารถพิเศษ";
      s.queueRank = i + 1;
      if (admittedSpTalent < baseQuotaSpTalent) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (ความสามารถพิเศษ)";
        admittedSpTalent++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - baseQuotaSpTalent} (ความสามารถพิเศษ)`;
      }
    });

    // 2. Special Condition
    let gSpCond = students.filter(s => s.appType === "เงื่อนไข");
    gSpCond.sort(sortStudents);
    let admittedSpCond = 0;
    gSpCond.forEach((s, i) => {
      s.queueGroup = "กลุ่มเงื่อนไขพิเศษ";
      s.queueRank = i + 1;
      if (admittedSpCond < baseQuotaSpCond) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (เงื่อนไขพิเศษ)";
        admittedSpCond++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - baseQuotaSpCond} (เงื่อนไขพิเศษ)`;
      }
    });
    let leftoverSpCond = Math.max(0, baseQuotaSpCond - admittedSpCond);

    // 3. In-zone
    let quotaIn = baseQuotaIn;
    let gIn = students.filter(s => s.appType === "ในเขต");
    gIn.sort(sortStudents);
    let admittedIn = 0;
    gIn.forEach((s, i) => {
      s.queueGroup = "กลุ่มในเขต";
      s.queueRank = i + 1;
      if (admittedIn < quotaIn) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (กลุ่มในเขต)";
        admittedIn++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - quotaIn} (กลุ่มในเขต)`;
      }
    });

    // 4. Out-zone
    let quotaOut = baseQuotaOut;
    let gOut = students.filter(s => ["นอกเขต", "ทั่วไป"].includes(s.appType));
    gOut.sort(sortStudents);
    let admittedOut = 0;
    gOut.forEach((s, i) => {
      s.queueGroup = "กลุ่มนอกเขต";
      s.queueRank = i + 1;
      if (admittedOut < quotaOut) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (กลุ่มนอกเขต)";
        admittedOut++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - quotaOut} (กลุ่มนอกเขต)`;
      }
    });

  } else {
    // M.4 Logic
    let baseQuotaSpTalent = quotaSettings["พิเศษ"] || 0;
    let baseQuotaSpCond   = quotaSettings["เงื่อนไข"] || 0;
    let baseQuotaOld      = quotaSettings["เดิม"] || 0;
    let baseQuotaOther    = quotaSettings["อื่น"] || 0;

    // 1. Special Talent
    let gSpTalent = students.filter(s => s.appType === "พิเศษ");
    gSpTalent.sort(sortStudents);
    let admittedSpTalent = 0;
    gSpTalent.forEach((s, i) => {
      s.queueGroup = "กลุ่มความสามารถพิเศษ";
      s.queueRank = i + 1;
      if (admittedSpTalent < baseQuotaSpTalent) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (ความสามารถพิเศษ)";
        admittedSpTalent++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - baseQuotaSpTalent} (ความสามารถพิเศษ)`;
      }
    });
    // 2. Special Condition
    let gSpCond = students.filter(s => s.appType === "เงื่อนไข");
    gSpCond.sort(sortStudents);
    let admittedSpCond = 0;
    gSpCond.forEach((s, i) => {
      s.queueGroup = "กลุ่มเงื่อนไขพิเศษ";
      s.queueRank = i + 1;
      if (admittedSpCond < baseQuotaSpCond) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (เงื่อนไขพิเศษ)";
        admittedSpCond++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - baseQuotaSpCond} (เงื่อนไขพิเศษ)`;
      }
    });

    // 3. Old students (Original)
    let quotaOld = baseQuotaOld;
    let gOld = students.filter(s => s.appType === "เดิม");
    gOld.sort(sortStudents);
    let admittedOld = 0;
    gOld.forEach((s, i) => {
      s.queueGroup = "กลุ่มเดิม";
      s.queueRank = i + 1;
      if (admittedOld < quotaOld) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (กลุ่มเดิม)";
        admittedOld++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - quotaOld} (กลุ่มเดิม)`;
      }
    });

    // 4. Other students
    let quotaOther = baseQuotaOther;
    let gOther = students.filter(s => ["อื่น", "ทั่วไป", "นอกเขต"].includes(s.appType));
    gOther.sort(sortStudents);
    let admittedOther = 0;
    gOther.forEach((s, i) => {
      s.queueGroup = "กลุ่มอื่น";
      s.queueRank = i + 1;
      if (admittedOther < quotaOther) {
        s._admitted = true;
        s.queueStatus = "ตัวจริง (กลุ่มอื่น)";
        admittedOther++;
      } else {
        s.queueStatus = `สำรองอันดับ ${s.queueRank - quotaOther} (กลุ่มอื่น)`;
      }
    });
  }
  
  // Re-sort everything to systemRank for predictable payload structure
  students.sort((a,b) => a.systemRank - b.systemRank);

  return {
    queueData: students.map(s => ({
      id: s.id,
      name: s.name,
      school: s.school,
      appType: s.appType,
      systemRank: s.systemRank,
      totalScore: s.totalScore,
      math: s.scores.math,
      sci: s.scores.sci,
      thai: s.scores.thai,
      soc: s.scores.soc,
      eng: s.scores.eng,
      read: s.scores.read,
      gpa: s.gpa,
      queueGroup: s.queueGroup,
      queueStatus: s.queueStatus,
      queueRank: s.queueRank,
      isAdmitted: s._admitted,
      choices: s.choices,
      phone: s.phone || "",
      oldStudentId: s.oldStudentId || ""
    }))
  };
}

/**
 * Phase 3: กระบวนการรันเลขประจำตัวนักเรียนและจัดห้องใหม่
 * 1. อ่านข้อมูลจาก Class_By_Room_Final
 * 2. จัดลำดับ: ห้อง -> เพศ -> คะแนน
 * 3. รัน ID เริ่มจาก Start ID
 */
/**
 * ระยะที่ 3: กระบวนการรันเลขประจำตัวนักเรียนและจัดห้องใหม่ (ชายขึ้นก่อน)
 */
/**
 * โหลดข้อมูลนักเรียนสำหรับ Phase 3 จากชีต _Class_By_Room_Final
 */
/**
 * ดึง Map สำหรับค้นหารหัสประจำตัวเดิมจาก Sheet1
 * Key: เลขประจำตัวผู้สอบ (Exam ID)
 * Value: รหัสประจำตัวนักเรียนเดิม (Old Student ID)
 */
function getOldIdLookupMap() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName('Sheet1');
  if (!sourceSheet) return {};
  
  const data = sourceSheet.getDataRange().getValues();
  const headers = data[0] || [];
  
  // หาคอลัมน์ เลขประจำตัวผู้สอบ (มักเป็นคอลัมน์แรก)
  const idColIdx = 0; 
  // หาคอลัมน์ รหัสเดิม
  const oldIdColIdx = headers.findIndex(h => String(h).match(/รหัสเดิม|รหัสประจำตัวเดิม/i));
  
  const map = {};
  if (oldIdColIdx === -1) return map;
  
  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const examId = String(row[idColIdx]).trim();
    const oldId = String(row[oldIdColIdx]).trim();
    if (examId && oldId) {
      map[examId] = oldId;
    }
  }
  return map;
}

function getPhase3Students(level, sortByAlphabet = false, preferExistingFirst = false) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheetName = `${level}_Class_By_Room_Final`;
  const sheet = ss.getSheetByName(rawSheetName);
  if (!sheet) throw new Error(`ไม่พบชีต: ${rawSheetName}`);

  const data = sheet.getDataRange().getValues();
  const headersInRaw = data[0];
  
  // ค้นหา index ของ column ชื่อนักเรียน
  const nameColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'ชื่อ-นามสกุล' || hStr === 'ชื่อ-สกุล' || hStr === 'name' || hStr === 'ชื่อ';
  });
  
  // ค้นหา index ของ column ห้อง
  const roomColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'ห้อง' || hStr === 'room' || hStr === 'Room';
  });
  
  const scoreColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'คะแนนรวม(%)' || hStr.includes('คะแนนรวม') || hStr === 'score';
  });

  const schoolColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'โรงเรียนเดิม' || hStr === 'โรงเรียน';
  });
  const existingIdColIdx = headersInRaw.findIndex(h => String(h).trim() === 'มีอยู่');
  const newIdColIdx = headersInRaw.findIndex(h => String(h).trim() === 'ออกใหม่');

  // Lookup รหัสเดิมจาก Sheet1 สดๆ เผื่อในชีตนี้ไม่มีข้อมูล หรือข้อมูลผิด
  const oldIdLookup = getOldIdLookupMap();
  
  if (nameColIdx === -1) {
    throw new Error(`ไม่พบ column ชื่อนักเรียน ในชีต ${rawSheetName}`);
  }
  
  const students = data.slice(1).map(row => {
    let s = {};
    headersInRaw.forEach((h, i) => s[h] = row[i]);
    s.name = row[nameColIdx];
    if (roomColIdx >= 0) s.assignedRoom = row[roomColIdx];
    if (scoreColIdx >= 0) s.totalScore = row[scoreColIdx];
    if (schoolColIdx >= 0) s.school = row[schoolColIdx];
    s.existingId = existingIdColIdx >= 0 ? String(row[existingIdColIdx] || '').trim() : '';
    s.newIdDisplay = newIdColIdx >= 0 ? String(row[newIdColIdx] || '').trim() : '';
    
    // Lookup รหัสเดิมจาก Sheet1 โดยใช้เลขประจำตัวผู้สอบ (Examination ID)
    const examIdKey = String(s['เลขประจำตัว'] || s['เลขประจำตัวผู้สอบ'] || '').trim();
    s.oldStudentId = s.existingId || oldIdLookup[examIdKey] || "";
    s.idSource = s.oldStudentId ? 'existing' : 'new';
    return s;
  });

  // Filter out empty rows (rows without name)
  const filteredStudents = students.filter(s => s.name && String(s.name).trim() !== '');

  // ฟังก์ชันเช็คเพศจากคำนำหน้า
  const isBoy = (name) => {
    const nameStr = String(name).trim().toLowerCase();
    return /^(เด็กชาย|นาย|ด\.ช\.|ดช\.|mr\.?|master)/.test(nameStr) || 
           nameStr.includes('เด็กชาย') || 
           nameStr.startsWith('นาย');
  };

  // ฟังก์ชั่นดึงตัวเลขจากห้อง (มองข้ามตัวอักษรไป เอาแค่ตัวเลขทั้งหมด) เช่น "1-05" -> 105, "M1-01" -> 101
  const getRoomNumber = (room) => {
    const roomStr = String(room || '');
    // ดึงตัวเลขทั้งหมดแล้วนำมารวม
    const numbers = roomStr.replace(/\D/g, '');
    return numbers ? parseInt(numbers) : 999;
  };

  // เรียงลำดับตามการเลือก
  filteredStudents.sort((a, b) => {
    const ra = getRoomNumber(a.assignedRoom);
    const rb = getRoomNumber(b.assignedRoom);
    if (ra !== rb) {
      return ra - rb;
    }

    if (preferExistingFirst) {
      const sa = a.idSource === 'existing' ? 1 : 2;
      const sb = b.idSource === 'existing' ? 1 : 2;
      if (sa !== sb) return sa - sb;
    }

    // เรียงตามเพศ (ชายก่อน) ก่อนเสมอ
    const ga = isBoy(a.name) ? 1 : 2;
    const gb = isBoy(b.name) ? 1 : 2;
    if (ga !== gb) return ga - gb;

    if (sortByAlphabet) {
      // เรียงตามตัวอักษรภาษาไทย
      const nameA = String(a.name || '').trim();
      const nameB = String(b.name || '').trim();
      return nameA.localeCompare(nameB, 'th');
    } else {
      // เรียงตามคะแนน (มากไปน้อย)
      return (parseFloat(b.totalScore) || 0) - (parseFloat(a.totalScore) || 0);
    }
  });

  // ส่งเฉพาะข้อมูลที่จำเป็นสำหรับ animation
  return filteredStudents.map(s => ({
    name: String(s.name || '(ไม่มีชื่อ)'),
    school: String(s.school || '-'),
    oldStudentId: String(s.oldStudentId || ''),
    idSource: String(s.idSource || 'new'),
    assignedRoom: String(s.assignedRoom || 'ไม่ระบุ'),
    totalScore: s.totalScore ? Number(s.totalScore).toFixed(3) : '0'
  }));
}

function processPhase3Ids(level, startId, sortByAlphabet = false, preferExistingFirst = false) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const rawSheetName = `${level}_Class_By_Room_Final`;
  const sheet = ss.getSheetByName(rawSheetName);
  if (!sheet) throw new Error(`ไม่พบชีต: ${rawSheetName}`);

  const data = sheet.getDataRange().getValues();
  const headersInRaw = data[0];
  
  // ค้นหา index ของ column ต่าง ๆ
  const nameColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'ชื่อ-นามสกุล' || hStr === 'ชื่อ-สกุล' || hStr === 'name' || hStr === 'ชื่อ';
  });
  
  const roomColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'ห้อง' || hStr === 'room' || hStr === 'Room';
  });
  
  const scoreColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'คะแนนรวม(%)' || hStr.includes('คะแนนรวม') || hStr === 'score';
  });
  
  const schoolColIdx = headersInRaw.findIndex(h => {
    const hStr = String(h).trim();
    return hStr === 'โรงเรียนเดิม' || hStr === 'โรงเรียน';
  });
  const existingIdColIdx = headersInRaw.findIndex(h => String(h).trim() === 'มีอยู่');
  const newIdColIdx = headersInRaw.findIndex(h => String(h).trim() === 'ออกใหม่');
  
  // Lookup รหัสเดิมจาก Sheet1 สดๆ
  const oldIdLookup = getOldIdLookupMap();

  if (nameColIdx === -1) {
    throw new Error(`ไม่พบ column ชื่อนักเรียน ในชีต ${rawSheetName}`);
  }
  
  const students = data.slice(1).map(row => {
    let s = {};
    headersInRaw.forEach((h, i) => s[h] = row[i]);
    s.name = row[nameColIdx];
    if (roomColIdx >= 0) s.assignedRoom = row[roomColIdx];
    if (scoreColIdx >= 0) s.totalScore = row[scoreColIdx];
    if (schoolColIdx >= 0) s.school = row[schoolColIdx];
    s.existingId = existingIdColIdx >= 0 ? String(row[existingIdColIdx] || '').trim() : '';
    s.newIdDisplay = newIdColIdx >= 0 ? String(row[newIdColIdx] || '').trim() : '';
    
    // Lookup รหัสเดิมจาก Sheet1 โดยใช้เลขประจำตัวผู้สอบ (Examination ID)
    const examIdKey = String(s['เลขประจำตัว'] || s['เลขประจำตัวผู้สอบ'] || '').trim();
    s.oldStudentId = s.existingId || oldIdLookup[examIdKey] || "";
    s.idSource = s.oldStudentId ? 'existing' : 'new';
    return s;
  });

  // Filter out empty rows
  const filteredStudents = students.filter(s => s.name && String(s.name).trim() !== '');

  // ฟังก์ชันดึงตัวเลขจากห้อง
  const getRoomNumber = (room) => {
    const roomStr = String(room || '');
    const numbers = roomStr.replace(/\D/g, '');
    return numbers ? parseInt(numbers) : 999;
  };

  // ฟังก์ชันเช็คเพศจากคำนำหน้า
  const isBoy = (name) => {
    const nameStr = String(name).trim().toLowerCase();
    return /^(เด็กชาย|นาย|ด\.ช\.|ดช\.|mr\.?|master)/.test(nameStr) || 
           nameStr.includes('เด็กชาย') || 
           nameStr.startsWith('นาย');
  };

  // เรียงลำดับตามความต้องการของผู้ใช้ (ห้อง -> เพศ -> คะแนน)
  filteredStudents.sort((a, b) => {
    const ra = getRoomNumber(a.assignedRoom);
    const rb = getRoomNumber(b.assignedRoom);
    if (ra !== rb) return ra - rb;

    if (preferExistingFirst) {
      const sa = a.idSource === 'existing' ? 1 : 2;
      const sb = b.idSource === 'existing' ? 1 : 2;
      if (sa !== sb) return sa - sb;
    }

    const ga = isBoy(a.name) ? 1 : 2;
    const gb = isBoy(b.name) ? 1 : 2;
    if (ga !== gb) return ga - gb;

    if (sortByAlphabet) {
      const nameA = String(a.name || '').trim();
      const nameB = String(b.name || '').trim();
      return nameA.localeCompare(nameB, 'th');
    } else {
      return (parseFloat(b.totalScore) || 0) - (parseFloat(a.totalScore) || 0);
    }
  });

  // Stage 1: กำหนดรหัสประจำตัว (Official Enrollment ID)
  // (เรียงลำดับดั้งเดิม: ห้อง -> เพศ -> คะแนน/ชื่อ เพื่อแจกรหัสใหม่ตามลำดับความเก่ง/ลำดับคิวเดิม สำหรับเด็กใหม่)
  let currentNewId = parseInt(startId) || 10001;
  filteredStudents.forEach(s => {
    if (s.existingId && s.existingId.trim() !== "") {
        s._finalId = s.existingId;
    } else if (s.oldStudentId && s.oldStudentId.trim() !== "") {
        s._finalId = s.oldStudentId;
    } else {
        s._finalId = String(currentNewId++);
    }
  });

  // Stage 2: เรียงลำดับเพื่อลง "เลขที่" (Seat Number)
  // ผู้ใช้ต้องการ: 1. เรียงตามห้อง, 2. แยกชาย-หญิง (ชายก่อน), 3. เรียงตาม "รหัสประจำตัวนักเรียน" (น้อยไปมาก)
  filteredStudents.sort((a, b) => {
    // 1. ห้อง
    const ra = getRoomNumber(a.assignedRoom);
    const rb = getRoomNumber(b.assignedRoom);
    if (ra !== rb) return ra - rb;

    // 2. เพศ (ชาย 1, หญิง 2)
    const ga = isBoy(a.name) ? 1 : 2;
    const gb = isBoy(b.name) ? 1 : 2;
    if (ga !== gb) return ga - gb;

    // 3. รหัสประจำตัวนักเรียน (เปรียบเทียบแบบตัวเลข)
    return String(a._finalId).localeCompare(String(b._finalId), undefined, { numeric: true });
  });

  const outputSheetName = `${level}_Official_Enrollment_Final`;
  let outSheet = ss.getSheetByName(outputSheetName);
  if (!outSheet) outSheet = ss.insertSheet(outputSheetName); else outSheet.clear();

  const headers = ['ห้อง', 'เลขที่', 'รหัสประจำตัวนักเรียน', 'ชื่อ-นามสกุล'];
  let outputRows = [headers];
  let roomCounters = {}; 

  // Stage 3: แจกเลขที่ตามลำดับการเรียงใหม่
  filteredStudents.forEach(s => {
    const room = String(s.assignedRoom || 'ไม่ระบุ');
    if (!roomCounters[room]) roomCounters[room] = 0;
    roomCounters[room]++;
    
    outputRows.push([
      room,
      roomCounters[room],
      s._finalId,
      String(s.name)
    ]);
  });

  outSheet.getRange(1, 1, outputRows.length, headers.length).setValues(outputRows);
  outSheet.getRange(1, 1, 1, headers.length).setFontWeight('bold').setBackground('#c2410c').setFontColor('white');
  
  const sortMethod = sortByAlphabet ? 'ตัวอักษรภาษาไทย' : 'เพศ (ชายก่อน)';
  const extraRule = preferExistingFirst ? ', มีอยู่ก่อนออกใหม่' : '';
  return `รันเลขประจำตัวเสร็จสมบูรณ์! (เรียงตาม: ${sortMethod}${extraRule}, รวม: ${filteredStudents.length} คน) - แผ่นงาน: ${outputSheetName}`;
}

function getPhase25Data(levelStr) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const prefix = levelStr ? `${levelStr}_` : '';
  const sheetName = `${prefix}Master_Processed_Final`;
  const sheet = ss.getSheetByName(sheetName);
  
  if (!sheet) throw new Error(`ไม่พบชีต: ${sheetName}`);

  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];

  const pNames = ['วิทยาศาสตร์-คณิตศาสตร์', 'วิทยาศาสตร์พลังสิบ', 'ศิลป์-คำนวณ', 'ศิลป์-ภาษา'];
  
  return data.slice(1).map(row => {
    let choiceArray = [null, null, null, null];
    [6, 7, 8, 9].forEach((colIdx, i) => {
      let val = parseInt(row[colIdx]);
      if (!isNaN(val) && val >= 1 && val <= 4) {
        choiceArray[val-1] = pNames[i];
      }
    });
    
    let choices = choiceArray.filter(c => c !== null);

    return {
      systemRank: parseInt(row[0]) || 0,
      id: row[1],
      name: row[2],
      school: row[3] || '-',
      levelCol: row[4],
      appType: row[5] || '-',
      choices: choices,
      gpa: parseFloat(row[10]) || 0,
      scores: {
        math: parseFloat(row[11]) || 0,
        sci: parseFloat(row[12]) || 0,
        thai: parseFloat(row[13]) || 0,
        soc: parseFloat(row[14]) || 0,
        eng: parseFloat(row[15]) || 0,
        read: parseFloat(row[16]) || 0
      },
      totalScore: parseFloat(row[17]) || 0,
      status: row[18] || '',
      assignedPlan: row[19] !== '-' ? row[19] : null,
      assignedRoom: row[20] !== '-' ? row[20] : null,
      remark: row[21] || '',
      oldStudentId: row[22] || '',
      phone: row[23] || '',
      isAdmitted: row[18] && row[18].includes('ตัวจริง')
    };
  });
}
