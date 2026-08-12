// frontend/src/app/(siswa)/exams/[id]/instructions/page.tsx
"use client";

import { useEffect, useState } from "react";
import { useParams, useRouter } from "next/navigation";
import { AppShell } from "@/components/layout/app-shell";
import { Card, CardHeader, CardTitle, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/siswa/EmptyState";
import { academicService } from "@/services/academic.service";
import { ExamPackageDetail } from "@/types/siswa";
import {
    ClipboardList,
    ListChecks,
    Timer,
    ShieldAlert,
    CheckCircle2,
    Loader2,
    PackageSearch,
    Layers,
    Award,
    Play,
} from "lucide-react";

export default function ExamInstructionsPage() {
    const params = useParams();
    const router = useRouter();
    const id = params?.id as string;

    const [pkg, setPkg] = useState<ExamPackageDetail | null>(null);
    const [loading, setLoading] = useState(true);
    const [notFound, setNotFound] = useState(false);
    const [agreed, setAgreed] = useState(false);
    const [starting, setStarting] = useState(false);
    const [startError, setStartError] = useState<string | null>(null);

    useEffect(() => {
        async function load() {
            setLoading(true);
            try {
                const detail = await academicService.getExamPackageDetail(id);
                if (detail && detail.id) {
                    setPkg(detail);
                } else {
                    setNotFound(true);
                }
            } catch {
                try {
                    const exam = await academicService.getExamById(id);
                    if (exam && exam.id) {
                        setPkg({
                            id: exam.id,
                            code: exam.category ?? "TRYOUT",
                            name: exam.title,
                            education_level: exam.grade_level ?? "SMA",
                            total_questions: exam.total_questions,
                            duration_minutes: exam.duration_minutes,
                            passing_score: exam.passing_score,
                            package_mode: "SINGLE",
                            subjects: Array.isArray(exam.subtests)
                                ? exam.subtests.map((s, i) => ({
                                      exam_content_id: s.id ?? `${exam.id}-${i}`,
                                      subject_name: s.subtest_name,
                                      display_order: i + 1,
                                  }))
                                : [],
                        } as ExamPackageDetail);
                    } else {
                        setNotFound(true);
                    }
                } catch {
                    setNotFound(true);
                }
            } finally {
                setLoading(false);
            }
        }
        load();
    }, [id]);

    const totalSubjects = pkg?.subjects?.length ?? pkg?.subjects_count ?? 0;
    const totalQuestions = pkg?.total_questions ?? 0;
    const durationMinutes = pkg?.duration_minutes ?? 0;
    const passingScore = pkg?.passing_score;

    const startExamId = pkg?.subjects?.[0]?.exam_content_id || id;

    const handleStart = async () => {
        setStarting(true);
        setStartError(null);
        try {
            const session = await academicService.startCBTExam(startExamId);
            if (session && session.id) {
                router.push(`/exams/${id}/cbt?session_id=${session.id}`);
            } else {
                setStartError("Sesi ujian gagal dibuat. Silakan coba lagi.");
                setStarting(false);
            }
        } catch (err: unknown) {
            const msg =
                err instanceof Error && err.message
                    ? err.message.toLowerCase().includes("attempt")
                        ? "Kesempatan mengerjakan sudah habis (max attempt mencapai limit)."
                        : err.message
                    : "Sesi ujian gagal dibuat. Silakan coba lagi.";
            setStartError(msg);
            setStarting(false);
        }
    };

    if (loading) {
        return (
            <AppShell>
                <div className="max-w-[640px] mx-auto">
                    <Skeleton className="h-96 w-full rounded-3xl" />
                </div>
            </AppShell>
        );
    }

    if (notFound || !pkg) {
        return (
            <AppShell>
                <div className="max-w-3xl mx-auto">
                    <EmptyState
                        icon={PackageSearch}
                        title="Paket Tidak Ditemukan"
                        description="Paket ujian ini tidak tersedia." 
                        actionLabel="Kembali ke Daftar Try Out"
                        actionHref="/exams"
                    />
                </div>
            </AppShell>
        );
    }

    return (
        <AppShell>
            <div className="max-w-[640px] mx-auto">
                <Card className="rounded-3xl border-border shadow-lg">
                    <CardHeader className="pb-4">
                        <div className="flex items-center gap-3">
                            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                                <ClipboardList className="h-5 w-5" />
                            </div>
                            <div>
                                <CardTitle className="font-heading text-lg font-semibold">Petunjuk Ujian</CardTitle>
                                <p className="text-xs text-muted-foreground">{pkg.name}</p>
                            </div>
                        </div>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="grid grid-cols-2 gap-3 text-sm">
                            <div className="flex items-center gap-2 rounded-xl border border-border bg-muted/30 px-3 py-2.5">
                                <ListChecks className="h-4 w-4 text-primary" />
                                <span className="text-muted-foreground text-xs">Jumlah soal</span>
                                <strong className="ml-auto font-bold">{totalQuestions || "—"}</strong>
                            </div>
                            <div className="flex items-center gap-2 rounded-xl border border-border bg-muted/30 px-3 py-2.5">
                                <Layers className="h-4 w-4 text-primary" />
                                <span className="text-muted-foreground text-xs">Sub-test</span>
                                <strong className="ml-auto font-bold">{totalSubjects || "—"}</strong>
                            </div>
                            <div className="flex items-center gap-2 rounded-xl border border-border bg-muted/30 px-3 py-2.5">
                                <Timer className="h-4 w-4 text-primary" />
                                <span className="text-muted-foreground text-xs">Durasi</span>
                                <strong className="ml-auto font-bold">{durationMinutes ? `${durationMinutes} menit` : "—"}</strong>
                            </div>
                            {passingScore != null && (
                                <div className="flex items-center gap-2 rounded-xl border border-border bg-muted/30 px-3 py-2.5">
                                    <Award className="h-4 w-4 text-primary" />
                                    <span className="text-muted-foreground text-xs">Batas lulus</span>
                                    <strong className="ml-auto font-bold">{passingScore}</strong>
                                </div>
                            )}
                        </div>

                        <ul className="space-y-2.5 text-sm text-muted-foreground">
                            <li className="flex items-start gap-2.5">
                                <CheckCircle2 className="mt-0.5 h-4 w-4 text-emerald-500 shrink-0" />
                                Timer berjalan otomatis setelah Anda menyetujui aturan.
                            </li>
                            <li className="flex items-start gap-2.5">
                                <CheckCircle2 className="mt-0.5 h-4 w-4 text-emerald-500 shrink-0" />
                                Bebas berpindah nomor soal; tidak ada penalti untuk jawaban kosong.
                            </li>
                            <li className="flex items-start gap-2.5">
                                <CheckCircle2 className="mt-0.5 h-4 w-4 text-emerald-500 shrink-0" />
                                Jawaban tersimpan otomatis setiap Anda memilih opsi.
                            </li>
                            <li className="flex items-start gap-2.5">
                                <ShieldAlert className="mt-0.5 h-4 w-4 text-amber-500 shrink-0" />
                                Dilarang pindah tab / keluar layar — pelanggaran dicatat.
                            </li>
                        </ul>

                        <label className="flex items-start gap-3 rounded-xl border border-border bg-muted/30 px-4 py-3 cursor-pointer">
                            <input
                                type="checkbox"
                                checked={agreed}
                                onChange={(e) => setAgreed(e.target.checked)}
                                className="mt-0.5 h-4 w-4 rounded border-border text-primary focus:ring-primary"
                            />
                            <span className="text-sm text-foreground">
                                Saya siap mematuhi semua aturan ujian di atas.
                            </span>
                        </label>

                        {startError && (
                            <p className="text-xs text-red-600 dark:text-red-400">{startError}</p>
                        )}

                        <div className="flex items-center justify-between gap-3 pt-1">
                            <Button variant="outline" className="rounded-xl" onClick={() => router.back()}>
                                Tidak, kembali
                            </Button>
                            <Button
                                className="rounded-xl gap-2"
                                disabled={!agreed || starting}
                                onClick={handleStart}
                            >
                                {starting ? (
                                    <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                    <Play className="h-4 w-4" />
                                )}
                                Setuju, Mulai Ujian
                            </Button>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </AppShell>
    );
}